package memcache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

// ErrCloseConnection returns by Handler when it needs to close connection
var ErrCloseConnection = errors.New("Close")

// Handler represents memcache protocol handler
type Handler interface {
	ServeMemcache(*Request, *Response) error
}

// HandlerFunc is an adapter to use function as Handler interface
type HandlerFunc func(*Request, *Response) error

// ServeMemcache calls h(req, res)
func (h HandlerFunc) ServeMemcache(req *Request, res *Response) error { return h(req, res) }

// HandleConnection handles client connection
func HandleConnection(c net.Conn, h Handler) {
	con := newConn(c)
	con.run(h)
}

// Request represents general memcache request
type Request struct {
	Command string
	Args    []string

	conn *connection
}

// Response represents general memcache response
type Response struct {
	conn *connection
}

// ReadBody reads request body. Handler MUST call ReadBody if command has body.
func (r *Request) ReadBody(length int) ([]byte, error) { return r.conn.readRequestBody(length) }

// Status sends one-line response to client.
func (r *Response) Status(status string) error {
	r.conn.WriteString(status + eol)
	return r.conn.Flush()
}

// UnknownCommandError sends "ERROR" response to client.
func (r *Response) UnknownCommandError() error { return r.Status("ERROR") }

// ClientError sends "CLIENT_ERROR " + msg response to client.
func (r *Response) ClientError(msg string) error { return r.Status("CLIENT_ERROR " + msg) }

// ServerError sends "SERVER_ERROR " + msg response to client.
func (r *Response) ServerError(msg string) error { return r.Status("SERVER_ERROR " + msg) }

// NotFound sends "NOT_FOUND" response to client.
func (r *Response) NotFound() error { return r.Status("NOT_FOUND") }

// Value sends "VALUE" response to client with body and zero arguments (flags = 0, cas = 0).
func (r *Response) Value(key string, body []byte) error { return r.ValueFull(key, body, 0, 0) }

// ValueFull sends "VALUE" response to client with body and all arguments.
func (r *Response) ValueFull(key string, body []byte, flags uint32, cas uint64) error {
	fmt.Fprintf(r.conn, "VALUE %s %d %d %d", key, flags, len(body), cas)
	r.conn.WriteString(eol)
	r.conn.Write(body)
	r.conn.WriteString(eol)
	return r.conn.Flush()
}

/////////////////////////

const eol = "\r\n"

var (
	errInvalidEOL = errors.New("invalid EOL (must be '\\r\\n'")
)

type connection struct {
	*bufio.ReadWriter
	conn net.Conn
}

func newConn(c net.Conn) *connection {
	return &connection{
		ReadWriter: bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c)),
		conn:       c,
	}
}

func (c *connection) Close() { c.conn.Close() }

func (c *connection) run(handler Handler) {
	defer c.Close()
	for {
		req, err := c.readRequestLine()
		if err != nil {
			break
		}
		if req.Command == "" {
			continue
		}
		err = handler.ServeMemcache(req, &Response{c})
		if err != nil {
			break
		}
	}
}

func (c *connection) readRequestLine() (*Request, error) {
	line, err := c.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if line[len(line)-len(eol):] != eol {
		return nil, errInvalidEOL
	}
	parts := strings.Split(line[:len(line)-len(eol)], " ")
	if len(parts) == 0 {
		parts = append(parts, "")
	}
	return &Request{
		Command: parts[0],
		Args:    parts[1:],
		conn:    c,
	}, nil
}

func (c *connection) readRequestBody(length int) ([]byte, error) {
	body := make([]byte, length+len(eol))
	if _, err := io.ReadFull(c, body); err != nil {
		return nil, err
	}
	if string(body[length:]) != eol {
		return nil, errInvalidEOL
	}
	return body[:length], nil
}
