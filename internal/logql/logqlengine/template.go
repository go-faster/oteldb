package logqlengine

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode/utf8"

	"github.com/Masterminds/sprig/v3"
	"github.com/go-faster/errors"
)

func getTemplateBuffer() *bytes.Buffer {
	return bytes.NewBuffer(nil)
}

func compileTemplate(
	name, tmpl string,
	currentTimestamp func() time.Time,
	currentLine func() string,
) (*template.Template, error) {
	return template.New(name).
		Option("missingkey=zero").
		Funcs(tmplFunctions(currentTimestamp, currentLine)).
		Parse(tmpl)
}

var sprigFuncs = sprig.TxtFuncMap()

func tmplFunctions(
	currentTimestamp func() time.Time,
	currentLine func() string,
) template.FuncMap {
	funcMap := template.FuncMap{
		// olds functions deprecated.
		"ToLower":    strings.ToLower,
		"ToUpper":    strings.ToUpper,
		"Replace":    strings.Replace,
		"Trim":       strings.Trim,
		"TrimLeft":   strings.TrimLeft,
		"TrimRight":  strings.TrimRight,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"TrimSpace":  strings.TrimSpace,
		"regexReplaceAll": func(re string, s string, replacement string) (string, error) {
			r, err := regexp.Compile(re)
			if err != nil {
				return "", err
			}
			return r.ReplaceAllString(s, replacement), nil
		},
		"regexReplaceAllLiteral": func(re string, s string, replacement string) (string, error) {
			r, err := regexp.Compile(re)
			if err != nil {
				return "", err
			}
			return r.ReplaceAllLiteralString(s, replacement), nil
		},
		"count": func(re string, s string) (int, error) {
			r, err := regexp.Compile(re)
			if err != nil {
				return 0, err
			}
			matches := r.FindAllStringIndex(s, -1)
			return len(matches), nil
		},
		"urldecode":        url.QueryUnescape,
		"urlencode":        url.QueryEscape,
		"bytes":            convertBytes,
		"duration":         convertDuration,
		"duration_seconds": convertDuration,
		"unixEpochMillis": func(date time.Time) string {
			return strconv.FormatInt(date.UnixMilli(), 10)
		},
		"unixEpochNanos": func(date time.Time) string {
			return strconv.FormatInt(date.UnixNano(), 10)
		},
		"toDateInZone": func(tmpl, zone, input string) time.Time {
			loc, err := time.LoadLocation(zone)
			if err != nil {
				loc = time.UTC
			}
			t, _ := time.ParseInLocation(tmpl, input, loc)
			return t
		},
		"unixToTime": func(epoch string) (time.Time, error) {
			var ct time.Time
			l := len(epoch)
			i, err := strconv.ParseInt(epoch, 10, 64)
			if err != nil {
				return ct, errors.Wrapf(err, "parse unix timestamp %q", epoch)
			}
			switch l {
			case 5:
				return time.Unix(i*86400, 0), nil
			case 10:
				return time.Unix(i, 0), nil
			case 13:
				return time.UnixMilli(i), nil
			case 16:
				return time.UnixMicro(i), nil
			case 19:
				return time.Unix(0, i), nil
			default:
				return ct, errors.Wrapf(err, "invalid unix timestamp %q", epoch)
			}
		},
		"alignLeft":  alignLeft,
		"alignRight": alignRight,
	}

	for _, name := range []string{
		"b64enc",
		"b64dec",
		"lower",
		"upper",
		"title",
		"trunc",
		"substr",
		"contains",
		"hasPrefix",
		"hasSuffix",
		"indent",
		"nindent",
		"replace",
		"repeat",
		"trim",
		"trimAll",
		"trimSuffix",
		"trimPrefix",
		"int",
		"float64",
		"add",
		"sub",
		"mul",
		"div",
		"mod",
		"addf",
		"subf",
		"mulf",
		"divf",
		"max",
		"min",
		"maxf",
		"minf",
		"ceil",
		"floor",
		"round",
		"fromJson",
		"date",
		"toDate",
		"now",
		"unixEpoch",
		"default",
	} {
		f, ok := sprigFuncs[name]
		if !ok {
			panic(fmt.Sprintf("sprig function %q does not exist", name))
		}
		funcMap[name] = f
	}
	funcMap["__timestamp__"] = currentTimestamp
	funcMap["__line__"] = currentLine
	return funcMap
}

func alignLeft(count int, s string) string {
	if count < 0 {
		return s
	}

	l := 0
	for i := range s {
		l++
		if l > count {
			return s[:i]
		}
	}
	pad := count - l
	return s + strings.Repeat(" ", pad)
}

func alignRight(count int, s string) string {
	if count < 0 {
		return s
	}

	l := 0
	offset := 0
	for {
		i := len(s) - offset
		if i == 0 {
			break
		}

		_, size := utf8.DecodeLastRuneInString(s[:i])
		offset += size

		l++
		if l > count {
			return s[i:]
		}
	}
	pad := count - l
	return strings.Repeat(" ", pad) + s
}
