package simplemmc

import (
	"errors"
	"net"
	"strconv"

	"github.com/davidmz/memcache"
)

type SetMode int

const (
	Set SetMode = iota
	Add
	Replace
)

var (
	ErrNotFound  = errors.New("NOT_FOUND")
	ErrNotStored = errors.New("NOT_STORED")
	ErrExists    = errors.New("EXISTS")
)

type Handler interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte, mode SetMode) error
	Del(key string) error
}

func Serve(addr string, h Handler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	h2 := fullHandler(h)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go memcache.HandleConnection(conn, h2)
	}
}

func fullHandler(h Handler) memcache.Handler {
	return memcache.HandlerFunc(func(req *memcache.Request, resp *memcache.Response) error {
		switch req.Command {
		case "get", "gets":
			if len(req.Args) == 0 {
				resp.ClientError("key required")
				return
			}
			if req.Command == "get" {
				req.Args = req.Args[:1]
			}

			for _, key := range req.Args {
				data, err := h.Get(key)
				if err == ErrNotFound {
					// do nothing
				} else if err != nil {
					resp.ServerError(err.Error())
					return
				} else {
					resp.Value(key, data)
				}
			}
			resp.Status("END")

		case "set", "add", "replace":
			if len(req.Args) < 4 {
				resp.ClientError("invalid command format")
				return
			}

			noreply := len(req.Args) == 5 && req.Args[4] == "noreply"

			bodyLen, err := strconv.Atoi(req.Args[3])
			if err != nil || bodyLen < 0 {
				if !noreply {
					resp.ClientError("invalid data length")
				}
				return
			}
			body, err := req.ReadBody(bodyLen)
			if err != nil {
				if !noreply {
					resp.ServerError(err.Error())
				}
				return
			}

			var mode SetMode
			switch req.Command {
			case "set":
				mode = Set
			case "add":
				mode = Add
			case "replace":
				mode = Replace
			}

			err = h.Set(req.Args[0], body, mode)
			if !noreply {
				if err == ErrNotStored || err == ErrNotFound || err == ErrExists {
					resp.Status(err.Error())
				} else if err != nil {
					resp.ServerError(err.Error())
				} else {
					resp.Status("STORED")
				}
			}

		case "del":
			if len(req.Args) < 1 {
				resp.ClientError("invalid command format")
				return
			}

			noreply := len(req.Args) == 2 && req.Args[1] == "noreply"

			err := h.Del(req.Args[0])
			if !noreply {
				if err == ErrNotFound {
					resp.Status(err.Error())
				} else if err != nil {
					resp.ServerError(err.Error())
				} else {
					resp.Status("DELETED")
				}
			}

		case "version":
			resp.Status("VERSION " + MemcacheVersion)

		case "quit":
			return memcache.ErrCloseConnection

		default:
			resp.UnknownCommandError()
		}
	})
}
