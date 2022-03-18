package minirpc

import (
	"html/template"
	"net/http"
)

const debugText = `<html>
	<body>
	<title>MiniRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type DebugHTTP struct {
	server *Server
}

type DebugService struct {
	Name   string
	Method map[string]*methodType
}

func (server DebugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var services []*DebugService
	server.server.serviceMap.Range(func(namei, svci interface{}) bool {
		name := namei.(string)
		svc := svci.(*service)
		services = append(services, &DebugService{
			Name:   name,
			Method: svc.method,
		})
		return true
	})
	err := debug.Execute(w, services)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
