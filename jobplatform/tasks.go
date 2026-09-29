package jobplatform

import (
	"context"
	"fmt"
)

type Registry struct {
	tasks map[string]Task
}

func NewRegistry() *Registry {
	r := &Registry{tasks: make(map[string]Task)}
	r.MustRegister("noop", noopTask{})
	r.MustRegister("echo", echoTask{})
	r.MustRegister("fail", failTask{})
	return r
}

func (r *Registry) Register(name string, task Task) error {
	if name == "" {
		return fmt.Errorf("task name is required")
	}
	if task == nil {
		return fmt.Errorf("task %q is nil", name)
	}
	if _, exists := r.tasks[name]; exists {
		return fmt.Errorf("task %q already registered", name)
	}
	r.tasks[name] = task
	return nil
}

func (r *Registry) MustRegister(name string, task Task) {
	if err := r.Register(name, task); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(name string) (Task, error) {
	task, ok := r.tasks[name]
	if !ok {
		return nil, fmt.Errorf("unknown task type %q", name)
	}
	return task, nil
}

type noopTask struct{}

func (noopTask) Execute(context.Context, Job) (Result, error) {
	return Result{Message: "ok"}, nil
}

func (noopTask) RetrySafety(Job) RetrySafety { return RetrySafe }

type echoTask struct{}

func (echoTask) Execute(_ context.Context, job Job) (Result, error) {
	text, _ := job.Definition.Args["text"].(string)
	if text == "" {
		return Result{}, fmt.Errorf("echo requires non-empty string arg text")
	}
	return Result{Message: text}, nil
}

func (echoTask) RetrySafety(Job) RetrySafety { return RetrySafe }

type failTask struct{}

func (failTask) Execute(_ context.Context, job Job) (Result, error) {
	message, _ := job.Definition.Args["message"].(string)
	if message == "" {
		message = "intentional short failure"
	}
	return Result{}, fmt.Errorf("%s", message)
}

func (failTask) RetrySafety(Job) RetrySafety { return RetrySafe }
