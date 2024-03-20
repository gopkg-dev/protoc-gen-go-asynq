package main

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/amzapi/protoc-gen-go-asynq/asynq"
	"github.com/huandu/xstrings"
)

var asynqTemplate = `
{{$svrType := .ServiceType}}
{{$svrName := .ServiceName}}

const {{.ServiceType}}QueueName = "{{snakecase $svrType}}"

type {{.ServiceType}}TaskServer interface {
{{- range .MethodSets}}
	{{.Name}}(context.Context, *{{.Request}}) (error)
{{- end}}
}

func Register{{.ServiceType}}TaskServer(s *asynqx.Server, srv {{.ServiceType}}TaskServer) {
	{{- range .Methods}}
	s.HandleFunc("{{.Typename}}", _{{$svrType}}_{{.Name}}_Task_Handler(srv))
	{{- end}}
}

{{range .Methods}}
func _{{$svrType}}_{{.Name}}_Task_Handler(srv {{$svrType}}TaskServer) func(context.Context, *asynq.Task) error {
	return func(ctx context.Context, task *asynq.Task) error {
		var in {{.Request}}
		if err := {{ .PayloadType | getPayloadType}}.Unmarshal(task.Payload(), &in); err != nil {
			return err
		}
		err := srv.{{.Name}}(ctx, &in)
		return err
	}
}
{{end}}

type {{.ServiceType}}SvcTask struct {}
var {{.ServiceType}}Task {{.ServiceType}}SvcTask

{{range .MethodSets}}
func (j *{{$svrType}}SvcTask) {{.Name}}(in *{{.Request}}, opts ...asynq.Option) (*asynq.Task, error) {
	payload, err := {{ .PayloadType | getPayloadType}}.Marshal(in)
	if err != nil {
		return nil, err
	}
	{{- if .TimeOut }}
	opts = append(opts, asynq.Timeout({{.TimeOut}}* time.Second))
	{{- end}}
	{{- if .MaxRetry }}
	opts = append(opts, asynq.MaxRetry({{.MaxRetry}}))
	{{- end}}
	{{- if .Retention }}
	opts = append(opts, asynq.Retention({{.Retention}}* time.Second))
	{{- end}}
	{{- if .Unique }}
	opts = append(opts, asynq.Unique({{.Unique}}* time.Second))
	{{- end}}
	opts = append(opts, asynq.Queue({{$svrType}}QueueName))
	task := asynq.NewTask("{{.Typename}}", payload, opts...)
		return task, nil
	}
{{end}}

type {{.ServiceType}}TaskClient interface {
{{- range .MethodSets}}
	{{.Name}}(ctx context.Context, req *{{.Request}}, opts ...asynq.Option) (info *asynq.TaskInfo, err error) 
{{- end}}
}

type {{.ServiceType}}TaskClientImpl struct{
	cc *asynq.Client
}
	
func New{{.ServiceType}}TaskClient (client *asynq.Client) {{.ServiceType}}TaskClient {
	return &{{.ServiceType}}TaskClientImpl{client}
}

{{range .MethodSets}}
func (c *{{$svrType}}TaskClientImpl) {{.Name}}(ctx context.Context, in *{{.Request}}, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	task, err := {{$svrType}}Task.{{.Name}}(in, opts...)
	if err != nil {
		return nil, err
	}
	info, err := c.cc.Enqueue(task)
	if err != nil {
		return nil, err
	}
	return info, nil
}
{{end}}
`

type serviceDesc struct {
	ServiceType string // Greeter
	ServiceName string // helloworld.Greeter
	Metadata    string // api/helloworld/helloworld.proto
	Methods     []*methodDesc
	MethodSets  map[string]*methodDesc
}

type methodDesc struct {
	// method
	Name    string
	Num     int
	Request string
	Reply   string
	// asynq rule
	Typename    *string
	TimeOut     *int32
	MaxRetry    *int32
	Retention   *int32
	Unique      *int32
	PayloadType *asynq.Task_PayloadType
}

func (s *serviceDesc) execute() string {
	s.MethodSets = make(map[string]*methodDesc)
	for _, m := range s.Methods {
		s.MethodSets[m.Name] = m
	}
	buf := new(bytes.Buffer)
	tmpl, err := template.New("asynq").Funcs(map[string]interface{}{
		"lower":     strings.ToLower,
		"snakecase": xstrings.ToSnakeCase,
		"getPayloadType": func(t *asynq.Task_PayloadType) string {
			if t == nil {
				return "proto"
			}
			switch t.String() {
			case asynq.Task_Protobuf.String():
				return "proto"
			case asynq.Task_JSON.String():
				return "json"
			default:
				return "proto"
			}
		},
	}).Parse(strings.TrimSpace(asynqTemplate))
	if err != nil {
		panic(err)
	}
	if err := tmpl.Execute(buf, s); err != nil {
		panic(err)
	}
	return strings.Trim(buf.String(), "\r\n")
}
