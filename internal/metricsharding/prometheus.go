package metricsharding

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/yqlclient"
)

var _ storage.Queryable = (*Sharder)(nil)

// Querier returns a new Querier on the storage.
func (s *Sharder) Querier(mint, maxt int64) (storage.Querier, error) {
	var minTime, maxTime time.Time
	if mint > 0 {
		minTime = time.UnixMilli(mint)
	}
	if maxt > 0 {
		maxTime = time.UnixMilli(maxt)
	}
	return &querier{
		sharder: s,
		yc:      s.yc,
		yql:     s.yql,

		mint:           minTime,
		maxt:           maxTime,
		extractTenants: s.shardOpts.TenantFromLabels,
	}, nil
}

type querier struct {
	sharder *Sharder
	yc      yt.Client
	yql     *yqlclient.Client

	mint time.Time
	maxt time.Time

	extractTenants func(ctx context.Context, s *Sharder, matchers []*labels.Matcher) ([]TenantID, error)

	closers    []io.Closer
	closersMux sync.Mutex
}

var _ storage.Querier = (*querier)(nil)

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifetime of the querier.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (q *querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	tenants, err := q.extractTenants(ctx, q.sharder, matchers)
	if err != nil {
		return nil, nil, errors.Wrap(err, "extract tenants from labels")
	}

	qb, err := q.sharder.GetBlocksForQuery(ctx, tenants, q.mint, q.maxt)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get blocks to query")
	}

	resultCh := make(chan string, 24)
	attrPath := "/" + name + "/1"
	grp, grpCtx := errgroup.WithContext(ctx)
	// TODO(tdakkota): limit concurrency.
	// TODO(tdakkota): add an upper limit for size of result.
	for _, block := range qb.RecentAttributes {
		block := block
		grp.Go(func() error {
			ctx := grpCtx
			return q.queryDynamicLabelValues(ctx, block.Root, attrPath, resultCh)
		})
	}
	for _, block := range qb.RecentResource {
		block := block
		grp.Go(func() error {
			ctx := grpCtx
			return q.queryDynamicLabelValues(ctx, block.Root, attrPath, resultCh)
		})
	}
	grp.Go(func() error {
		ctx := grpCtx
		return queryStaticLabelValues(ctx, q, qb.Closed, Block.Attributes, attrPath, resultCh)
	})
	grp.Go(func() error {
		ctx := grpCtx
		return queryStaticLabelValues(ctx, q, qb.Closed, Block.Resource, attrPath, resultCh)
	})

	// Use map to dedup labels.
	result := map[string]struct{}{}
	grp.Go(func() error {
		ctx := grpCtx
		for {
			select {
			case <-ctx.Done():
				// Query caused an error/request was canceled.
				//
				// It's okay to not read from resultCh, since all writers would be
				// unblocked by canceled context.
				return ctx.Err()
			case v := <-resultCh:
				result[v] = struct{}{}
			}
		}
	})

	if err := grp.Wait(); err != nil {
		return nil, nil, err
	}
	return maps.Keys(result), nil, err
}

func queryStaticLabelValues[S any, G func(S) ypath.Path](
	ctx context.Context,
	q *querier,
	tables []S,
	getPath G,
	attrPath string,
	to chan<- string,
) error {
	type Row struct {
		Value string `yson:"value"`
	}

	var query strings.Builder
	fmt.Fprintf(&query, `SELECT Yson::ConvertToString(Yson::YPath(attrs, %q)) as value FROM CONCAT(`, attrPath)
	for i, table := range tables {
		if i != 0 {
			query.WriteString(",")
		}
		fmt.Fprintf(&query, `%#q`, getPath(table))
	}
	query.WriteByte(')')
	fmt.Fprintf(&query, "WHERE Yson::YPath(attrs, %q) IS NOT NULL", attrPath)

	iter, err := yqlclient.YQLQuery[Row](ctx, q.yql, query.String())
	if err != nil {
		return err
	}
	defer func() {
		_ = iter.Close()
	}()

	var row Row
	for iter.Next(&row) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case to <- row.Value:
		}
	}
	return iter.Err()
}

func (q *querier) queryDynamicLabelValues(ctx context.Context, table ypath.Path, attrPath string, to chan<- string) error {
	r, err := q.yc.SelectRows(ctx, fmt.Sprintf(`try_get_string(%[1]q) FROM [%[2]s] WHERE NOT is_null(try_get_string(%[1]q))`, table, attrPath), nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()

	var row struct {
		Value string `yson:"value"`
	}
	for r.Next() {
		if err := r.Scan(&row); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case to <- row.Value:
		}
	}

	return r.Err()
}

// LabelNames returns all the unique label names present in the block in sorted order.
// If matchers are specified the returned result set is reduced
// to label names of metrics matching the matchers.
func (q *querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	tenants, err := q.extractTenants(ctx, q.sharder, matchers)
	if err != nil {
		return nil, nil, errors.Wrap(err, "extract tenants from labels")
	}

	qb, err := q.sharder.GetBlocksForQuery(ctx, tenants, q.mint, q.maxt)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get blocks to query")
	}

	resultCh := make(chan pcommon.Map, 24)
	grp, grpCtx := errgroup.WithContext(ctx)
	// TODO(tdakkota): limit concurrency.
	// TODO(tdakkota): add an upper limit for size of result.
	for _, block := range qb.RecentAttributes {
		block := block
		grp.Go(func() error {
			ctx := grpCtx
			return q.queryDynamicLabels(ctx, block.Root, resultCh)
		})
	}
	for _, block := range qb.RecentResource {
		block := block
		grp.Go(func() error {
			ctx := grpCtx
			return q.queryDynamicLabels(ctx, block.Root, resultCh)
		})
	}
	grp.Go(func() error {
		ctx := grpCtx
		return queryStaticLabels(ctx, q, qb.Closed, Block.Attributes, resultCh)
	})
	grp.Go(func() error {
		ctx := grpCtx
		return queryStaticLabels(ctx, q, qb.Closed, Block.Resource, resultCh)
	})

	// Use map to dedup labels.
	result := map[string]struct{}{}
	grp.Go(func() error {
		ctx := grpCtx
		for {
			select {
			case <-ctx.Done():
				// Query caused an error/request was canceled.
				//
				// It's okay to not read from resultCh, since all writers would be
				// unblocked by canceled context.
				return ctx.Err()
			case v := <-resultCh:
				v.Range(func(k string, _ pcommon.Value) bool {
					result[k] = struct{}{}
					return true
				})
			}
		}
	})

	if err := grp.Wait(); err != nil {
		return nil, nil, err
	}
	return maps.Keys(result), nil, err
}

func queryStaticLabels[S any, G func(S) ypath.Path](
	ctx context.Context,
	q *querier,
	tables []S,
	getPath G,
	to chan<- pcommon.Map,
) error {
	type Row struct {
		Attrs otelstorage.Attrs `yson:"attrs"`
	}

	var query strings.Builder
	query.WriteString("SELECT attrs FROM CONCAT(")
	for i, table := range tables {
		if i != 0 {
			query.WriteString(",")
		}
		fmt.Fprintf(&query, `%#q`, getPath(table))
	}
	query.WriteByte(')')

	iter, err := yqlclient.YQLQuery[Row](ctx, q.yql, query.String())
	if err != nil {
		return err
	}
	defer func() {
		_ = iter.Close()
	}()

	var row Row
	for iter.Next(&row) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case to <- row.Attrs.AsMap():
		}
	}
	return iter.Err()
}

func (q *querier) queryDynamicLabels(ctx context.Context, table ypath.Path, to chan<- pcommon.Map) error {
	r, err := q.yc.SelectRows(ctx, fmt.Sprintf(`attrs FROM [%s]`, table), nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()

	var row struct {
		Attrs otelstorage.Attrs `yson:"attrs"`
	}
	for r.Next() {
		if err := r.Scan(&row); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case to <- row.Attrs.AsMap():
		}
	}

	return r.Err()
}

// Close releases the resources of the Querier.
func (q *querier) Close() (rerr error) {
	q.closersMux.Lock()
	defer q.closersMux.Unlock()

	for _, c := range q.closers {
		rerr = c.Close()
	}
	return rerr
}

func (q *querier) addCloser(c io.Closer) {
	q.closersMux.Lock()
	defer q.closersMux.Unlock()

	q.closers = append(q.closers, c)
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimizing select, but it's up to implementation how this is used if used at all.
func (q *querier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	set, err := q.selectSeries(ctx, hints, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return set
}

type query struct {
	Start    time.Time
	End      time.Time
	Matchers []*labels.Matcher
	Blocks   QueryBlocks
}

func (q *querier) selectSeries(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	tenants, err := q.extractTenants(ctx, q.sharder, matchers)
	if err != nil {
		return nil, errors.Wrap(err, "extract tenants from labels")
	}

	var (
		start = q.mint
		end   = q.maxt
	)
	if hints != nil {
		if t := time.UnixMilli(hints.Start); t.After(start) {
			start = t
		}
		if t := time.UnixMilli(hints.End); t.Before(end) {
			end = t
		}
	}

	qb, err := q.sharder.GetBlocksForQuery(ctx, tenants, start, end)
	if err != nil {
		return nil, errors.Wrap(err, "get blocks to query")
	}
	qry := &query{
		Start:    start,
		End:      end,
		Matchers: matchers,
		Blocks:   qb,
	}

	var (
		hashes   = map[otelstorage.Hash]otelstorage.Attrs{}
		resultCh = make(chan hashesResult, 2)
	)
	{
		grp, grpCtx := errgroup.WithContext(ctx)
		grp.Go(func() error {
			ctx := grpCtx
			return q.attributeHashes(ctx, resultCh, qry, false)
		})
		grp.Go(func() error {
			ctx := grpCtx
			return q.attributeHashes(ctx, resultCh, qry, true)
		})
		grp.Go(func() error {
			ctx := grpCtx
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case row := <-resultCh:
					hashes[row.Hash] = row.Attributes
				}
			}
		})
		if err := grp.Wait(); err != nil {
			return nil, errors.Wrap(err, "query hashes")
		}
	}

	return q.queryMetrics(ctx, qry, hashes)
}

func (q *querier) queryMetrics(ctx context.Context, qry *query, hashes map[otelstorage.Hash]otelstorage.Attrs) (_ storage.SeriesSet, rerr error) {
	var query strings.Builder

	// Collect tables to query.
	{
		query.WriteString("SELECT metric_name, attr_hash, resource_hash, timestamp, point FROM CONCAT(")
		for i, table := range qry.Blocks.Active {
			if i != 0 {
				query.WriteString(",")
			}
			fmt.Fprintf(&query, `%#q`, table)
		}
		for i, table := range qry.Blocks.Closed {
			if i != 0 {
				query.WriteString(",")
			}
			fmt.Fprintf(&query, `%#q`, table.Points())
		}
		query.WriteString(")\n")
	}

	// Set the clauses.
	{
		query.WriteString("WHERE\n")
		fmt.Fprintf(&query, "`timestamp` >= %d AND `timestamp` <= %d\n", qry.Start.UnixNano(), qry.End.UnixNano())

		var hashSet strings.Builder
		{
			hashSet.WriteString("(\n")
			i := 0
			for hash := range hashes {
				if i != 0 {
					hashSet.WriteString(",")
				}
				fmt.Fprintf(&hashSet, "%q", hash)
				i++
			}
			hashSet.WriteString(")\n")
		}

		fmt.Fprintf(&query, "AND (attr_hash IN %[1]s OR resource_hash IN %[1]s)", hashSet.String())
	}
	// Sort by series key, then by timestamp.
	query.WriteString("ORDER BY metric_name || attr_hash || resource_hash, timestamp ASC")

	iter, err := yqlclient.YQLQuery[metricstorage.Point](ctx, q.yql, query.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if rerr != nil {
			_ = iter.Close()
		} else {
			q.addCloser(iter)
		}
	}()

	return &seriesSet{
		iter:   iter,
		hashes: hashes,
	}, nil
}

type hashesResult struct {
	Hash       otelstorage.Hash  `yson:"hash"`
	Attributes otelstorage.Attrs `yson:"attrs"`
}

func (q *querier) attributeHashes(ctx context.Context, to chan<- hashesResult, qry *query, resource bool) error {
	var query strings.Builder

	// Pre-compile regex matchers.
	for matcherIdx, m := range qry.Matchers {
		switch m.Type {
		case labels.MatchRegexp, labels.MatchNotRegexp:
			fmt.Fprintf(&query, "$label_matcher_%d = Re2::Match(%q);\n", matcherIdx, m.Value)
		}
	}

	var (
		recentBlocks  = qry.Blocks.RecentAttributes
		getClosedPath = Block.Attributes
	)
	if resource {
		recentBlocks = qry.Blocks.RecentResource
		getClosedPath = Block.Resource
	}

	// Collect tables to query.
	query.WriteString("SELECT hash, attrs FROM CONCAT(")
	for i, table := range recentBlocks {
		if i != 0 {
			query.WriteString(",")
		}
		fmt.Fprintf(&query, `%#q`, table.Root)
	}
	for i, table := range qry.Blocks.Closed {
		if i != 0 {
			query.WriteString(",")
		}
		fmt.Fprintf(&query, `%#q`, getClosedPath(table))
	}
	query.WriteString(")\n")

	query.WriteString("WHERE\n")
	fmt.Fprintf(&query, "`timestamp` >= %d AND `timestamp` <= %d", qry.Start.UnixNano(), qry.End.UnixNano())

	// Preallocate path buffer.
	yp := make([]byte, 0, 32)
	for matcherIdx, m := range qry.Matchers {
		query.WriteString("\t(")

		sel := "metric_name"
		if m.Name != "__name__" {
			yp = append(yp[:0], '/')
			yp = append(yp, m.Name...)
			yp = append(yp, "/1"...)
			sel = fmt.Sprintf("Yson::ConvertToString(Yson::YPath(attrs, %q))", yp)
		}

		switch m.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			query.WriteString("AND ")
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			query.WriteString("AND NOT ")
		default:
			return errors.Errorf("unexpected type %q", m.Type)
		}

		// Note: predicate negated above.
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			fmt.Fprintf(&query, "%s = %q", sel, m.Value)
		case labels.MatchRegexp, labels.MatchNotRegexp:
			fmt.Fprintf(&query, "$label_matcher_%d(%s)", matcherIdx, sel)
		default:
			return errors.Errorf("unexpected type %q", m.Type)
		}
		query.WriteString(")\n")
	}

	iter, err := yqlclient.YQLQuery[hashesResult](ctx, q.yql, query.String())
	if err != nil {
		return err
	}
	defer func() {
		_ = iter.Close()
	}()

	var row hashesResult
	for iter.Next(&row) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case to <- row:
		}
	}
	return iter.Err()
}

type seriesKey struct {
	Name          otelstorage.Hash
	AttributeHash otelstorage.Hash
	ResourceHash  otelstorage.Hash
}

func getSeriesKey(p metricstorage.Point) seriesKey {
	return seriesKey{
		Name:          p.Metric,
		AttributeHash: p.AttributeHash,
		ResourceHash:  p.ResourceHash,
	}
}

type seriesSet struct {
	// iter from storage.
	//
	// expected to be sorted by [seriesKey] fields, then sorted by timestamp.
	iter iterators.Iterator[metricstorage.Point]

	// current series key.
	key seriesKey
	// current series points.
	points []metricstorage.Point

	// peeking iterator state.
	buf      metricstorage.Point
	buffered bool

	// hash -> attributes map.
	hashes map[otelstorage.Hash]otelstorage.Attrs
}

var _ storage.SeriesSet = (*seriesSet)(nil)

func (s *seriesSet) Next() bool {
	s.points = s.points[:0]

	// Read first point.
	first, ok := s.next()
	if !ok {
		// End of query result.
		return false
	}
	s.points = append(s.points, first)

	s.key = getSeriesKey(first)
loop:
	for {
		switch p, ok := s.peek(); {
		case !ok:
			// End of query result.
			break loop
		case s.key != getSeriesKey(p):
			// End of series.
			break loop
		default:
			s.points = append(s.points, p)
		}
	}
	// Sort points just in case.
	slices.SortFunc(s.points, func(a, b metricstorage.Point) int {
		return cmp.Compare(a.Timestamp, b.Timestamp)
	})

	// Check the iterator error.
	return s.iter.Err() == nil
}

func cmpPointTimestamp(a, b metricstorage.Point) int {
	return cmp.Compare(a.Timestamp, b.Timestamp)
}

func (s *seriesSet) next() (metricstorage.Point, bool) {
	p, ok := s.peek()
	if ok {
		// Force to read the value again.
		s.buffered = false
	}
	return p, ok
}

func (s *seriesSet) peek() (metricstorage.Point, bool) {
	if !s.buffered {
		s.buffered = s.iter.Next(&s.buf)
	}
	return s.buf, s.buffered
}

// At returns full series. Returned series should be iterable even after Next is called.
func (s *seriesSet) At() storage.Series {
	ls := labels.NewScratchBuilder(0)
	otelAttributesToProm(&ls, s.hashes[s.key.AttributeHash])
	otelAttributesToProm(&ls, s.hashes[s.key.ResourceHash])
	return &series{
		labels: ls.Labels(),
		points: s.points,
	}
}

func otelAttributesToProm(l *labels.ScratchBuilder, m otelstorage.Attrs) {
	if m.IsZero() {
		return
	}
	m.AsMap().Range(func(k string, v pcommon.Value) bool {
		l.Add(k, v.AsString())
		return true
	})
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (s *seriesSet) Err() error {
	return s.iter.Err()
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (s *seriesSet) Warnings() annotations.Annotations {
	return nil
}

type series struct {
	labels labels.Labels
	points []metricstorage.Point
}

var _ storage.Series = (*series)(nil)

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (s *series) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (s *series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return &pointIterator{
		data: s.points,
		n:    0,
	}
}

type pointIterator struct {
	data []metricstorage.Point
	n    int
}

var _ chunkenc.Iterator = (*pointIterator)(nil)

// Next advances the iterator by one and returns the type of the value
// at the new position (or ValNone if the iterator is exhausted).
func (p *pointIterator) Next() chunkenc.ValueType {
	if p.n >= len(p.data) {
		return chunkenc.ValFloat
	}
	p.n++
	return chunkenc.ValFloat
}

// Seek advances the iterator forward to the first sample with a
// timestamp equal or greater than t. If the current sample found by a
// previous `Next` or `Seek` operation already has this property, Seek
// has no effect. If a sample has been found, Seek returns the type of
// its value. Otherwise, it returns ValNone, after which the iterator is
// exhausted.
func (p *pointIterator) Seek(t int64) chunkenc.ValueType {
	seek := pcommon.NewTimestampFromTime(time.UnixMilli(t))

	// Find the closest value.
	idx, _ := slices.BinarySearchFunc(p.data, seek, func(p metricstorage.Point, seek pcommon.Timestamp) int {
		return cmp.Compare(p.Timestamp, seek)
	})
	if idx >= len(p.data) {
		return chunkenc.ValNone
	}
	p.n = idx
	return chunkenc.ValFloat
}

// At returns the current timestamp/value pair if the value is a float.
// Before the iterator has advanced, the behavior is unspecified.
func (p *pointIterator) At() (t int64, v float64) {
	at := p.data[p.n]
	return at.Timestamp.AsTime().UnixMilli(), at.Point
}

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts. Before the iterator has advanced,
// the behavior is unspecified.
func (p *pointIterator) AtHistogram() (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behavior is unspecified.
func (p *pointIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behavior is unspecified.
func (p *pointIterator) AtT() int64 {
	at := p.data[p.n]
	return at.Timestamp.AsTime().UnixMilli()
}

// Err returns the current error. It should be used only after the
// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
func (p *pointIterator) Err() error {
	return nil
}
