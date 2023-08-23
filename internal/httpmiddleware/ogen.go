package httpmiddleware

import "net/url"

// OgenServer is a generic ogen server type.
type OgenServer[R OgenRoute] interface {
	FindPath(method string, u *url.URL) (r R, _ bool)
}

// OgenRoute is a generic ogen route type.
type OgenRoute interface {
	Name() string
	OperationID() string
}

// RouteFinder finds Route by given URL.
type RouteFinder func(method string, u *url.URL) (OgenRoute, bool)

// MakeRouteFinder creates RouteFinder from given server.
func MakeRouteFinder[R OgenRoute, S OgenServer[R]](server S) RouteFinder {
	return func(method string, u *url.URL) (OgenRoute, bool) {
		return server.FindPath(method, u)
	}
}
