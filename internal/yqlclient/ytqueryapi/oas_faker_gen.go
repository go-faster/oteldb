// Code generated by ogen, DO NOT EDIT.

package ytqueryapi

import (
	"fmt"
	"time"

	"github.com/go-faster/jx"
)

// SetFake set fake values.
func (s *AbortedQuery) SetFake() {
}

// SetFake set fake values.
func (s *Attributes) SetFake() {
	{
		{
			s.Host.SetFake()
		}
	}
	{
		{
			s.Pid.SetFake()
		}
	}
	{
		{
			s.Tid.SetFake()
		}
	}
	{
		{
			s.Thread.SetFake()
		}
	}
	{
		{
			s.Fid.SetFake()
		}
	}
	{
		{
			s.Datetime.SetFake()
		}
	}
	{
		{
			s.TraceID.SetFake()
		}
	}
	{
		{
			s.SpanID.SetFake()
		}
	}
	{
		{
			s.ConnectionID.SetFake()
		}
	}
	{
		{
			s.RealmID.SetFake()
		}
	}
	{
		{
			s.Method.SetFake()
		}
	}
	{
		{
			s.RequestID.SetFake()
		}
	}
	{
		{
			s.Timeout.SetFake()
		}
	}
	{
		{
			s.Address.SetFake()
		}
	}
	{
		{
			s.Service.SetFake()
		}
	}
}

// SetFake set fake values.
func (s *Engine) SetFake() {
	*s = EngineYql
}

// SetFake set fake values.
func (s *Error) SetFake() {
	{
		{
			s.Code = int(0)
		}
	}
	{
		{
			s.Message = "string"
		}
	}
	{
		{
			s.Attributes.SetFake()
		}
	}
	{
		{
			s.InnerErrors = nil
			for i := 0; i < 0; i++ {
				var elem Error
				{
					elem.SetFake()
				}
				s.InnerErrors = append(s.InnerErrors, elem)
			}
		}
	}
}

// SetFake set fake values.
func (s *OperationState) SetFake() {
	*s = OperationStateRunning
}

// SetFake set fake values.
func (s *OptAttributes) SetFake() {
	var elem Attributes
	{
		elem.SetFake()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptDateTime) SetFake() {
	var elem time.Time
	{
		elem = time.Now()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptError) SetFake() {
	var elem Error
	{
		elem.SetFake()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptInt) SetFake() {
	var elem int
	{
		elem = int(0)
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptQueryStatusAnnotations) SetFake() {
	var elem QueryStatusAnnotations
	{
		elem.SetFake()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptQueryStatusProgress) SetFake() {
	var elem QueryStatusProgress
	{
		elem.SetFake()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptQueryStatusSettings) SetFake() {
	var elem QueryStatusSettings
	{
		elem.SetFake()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptString) SetFake() {
	var elem string
	{
		elem = "string"
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptUint64) SetFake() {
	var elem uint64
	{
		elem = uint64(0)
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *QueryID) SetFake() {
	var unwrapped string
	{
		unwrapped = "string"
	}
	*s = QueryID(unwrapped)
}

// SetFake set fake values.
func (s *QueryStatus) SetFake() {
	{
		{
			s.ID.SetFake()
		}
	}
	{
		{
			s.Engine.SetFake()
		}
	}
	{
		{
			s.StartTime = time.Now()
		}
	}
	{
		{
			s.FinishTime.SetFake()
		}
	}
	{
		{
			s.PingTime.SetFake()
		}
	}
	{
		{
			s.Settings.SetFake()
		}
	}
	{
		{
			s.User.SetFake()
		}
	}
	{
		{
			s.State.SetFake()
		}
	}
	{
		{
			s.ResultCount.SetFake()
		}
	}
	{
		{
			s.Progress.SetFake()
		}
	}
	{
		{
			s.Annotations.SetFake()
		}
	}
	{
		{
			s.Incarnation.SetFake()
		}
	}
	{
		{
			s.Error.SetFake()
		}
	}
}

// SetFake set fake values.
func (s *QueryStatusAnnotations) SetFake() {
	var (
		elem jx.Raw
		m    map[string]jx.Raw = s.init()
	)
	for i := 0; i < 0; i++ {
		m[fmt.Sprintf("fake%d", i)] = elem
	}
}

// SetFake set fake values.
func (s *QueryStatusProgress) SetFake() {
	var (
		elem jx.Raw
		m    map[string]jx.Raw = s.init()
	)
	for i := 0; i < 0; i++ {
		m[fmt.Sprintf("fake%d", i)] = elem
	}
}

// SetFake set fake values.
func (s *QueryStatusSettings) SetFake() {
	var (
		elem jx.Raw
		m    map[string]jx.Raw = s.init()
	)
	for i := 0; i < 0; i++ {
		m[fmt.Sprintf("fake%d", i)] = elem
	}
}

// SetFake set fake values.
func (s *StartedQuery) SetFake() {
	{
		{
			s.QueryID.SetFake()
		}
	}
}