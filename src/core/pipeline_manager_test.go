package core

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type MockEnvironment struct {
	vars map[string]string
}

func (e *MockEnvironment) Get(key string) (string, bool) {
	val, ok := e.vars[key]
	return val, ok
}

func TestPipelineManagerRegister(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE":  "root",
			"IMAGE":     "test-image",
			"NAMESPACE": "test-ns",
		},
	}

	pipelineManager := NewPipelineManager(clientset, env)

	pipeline1 := NewPipeline("test1", NewArraySource([]string{"data1"}, 1), NewArrayTarget[string](), nil)
	pipelineManager.RegisterSingle(pipeline1)

	pipeline2 := NewPipeline("test2", NewArraySource([]string{"data2"}, 1), NewArrayTarget[string](), nil)
	pipelineManager.RegisterMulti(pipeline2, 2)

	assert.Equal(t, 1, len(pipelineManager.singlePipelines))
	assert.Equal(t, 1, len(pipelineManager.multiPipelines))
}

func TestPipelineManagerRunRootMode(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE":  "root",
			"IMAGE":     "test-image",
			"NAMESPACE": "test-ns",
		},
	}

	pipelineManager := NewPipelineManager(clientset, env)

	pipelineManager.RegisterSingle(NewPipeline("test1", nil, nil, nil))

	pipelineManager.RegisterMulti(NewPipeline("test2", nil, nil, nil), 2)

	_, err := pipelineManager.Run()
	assert.Nil(t, err)

	deployments, err := clientset.AppsV1().Deployments("test-ns").List(context.Background(), metav1.ListOptions{})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(deployments.Items))

	for _, deployment := range deployments.Items {
		assert.Equal(t, "test-image", deployment.Spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, "pipeline", deployment.Spec.Template.Spec.Containers[0].Env[1].Value)
		if deployment.GetName() == "pipeline-test1" {
			assert.Equal(t, "test1", deployment.Spec.Template.Spec.Containers[0].Env[0].Value)
			assert.Equal(t, int32(1), *deployment.Spec.Replicas)
		} else if deployment.GetName() == "pipeline-test2" {
			assert.Equal(t, "test2", deployment.Spec.Template.Spec.Containers[0].Env[0].Value)
			assert.Equal(t, int32(2), *deployment.Spec.Replicas)
		}
	}
}

func TestPipelineManagerRunSinglePipeline(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE": "pipeline",
			"PIPELINE": "test1",
		},
	}

	pipelineManager := NewPipelineManager(clientset, env)

	source := NewArraySource([]string{"data"}, 1)
	target := NewArrayTarget[string]()
	pipeline1 := NewPipeline("test1", source, target, nil)
	pipelineManager.RegisterSingle(pipeline1)

	done, err := pipelineManager.Run()
	assert.Nil(t, err)

	<-done
	assert.Equal(t, []string{"data"}, target.array)

	assert.Equal(t, []string{"data"}, target.array)
}

func TestPipelineManagerRunMultiPipeline(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE": "pipeline",
			"PIPELINE": "test1",
		},
	}

	pipelineManager := NewPipelineManager(clientset, env)

	source := NewArraySource([]string{"data"}, 1)
	target := NewArrayTarget[string]()
	pipeline1 := NewPipeline("test1", source, target, nil)
	pipelineManager.RegisterMulti(pipeline1, 10)

	done, err := pipelineManager.Run()
	assert.Nil(t, err)

	<-done
	assert.Equal(t, []string{"data"}, target.array)

	assert.Equal(t, []string{"data"}, target.array)
}

func TestPipelineManagerRunInUnknownMode(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE": "unknown",
		},
	}

	pipelineManager := NewPipelineManager(clientset, env)

	_, err := pipelineManager.Run()
	assert.Equal(t, errors.New("unknown mode"), err)
}

func TestPipelineManagerRunUnknownPipeline(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE": "pipeline",
			"PIPELINE": "unknown",
		},
	}

	pipelineManager := NewPipelineManager(clientset, env)

	_, err := pipelineManager.Run()
	assert.Equal(t, errors.New("not found pipeline to run"), err)
}

func TestPipelientManagerRunUnknownImage(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE":  "root",
			"NAMESPACE": "test-ns",
		},
	}
	pipelineManager := NewPipelineManager(clientset, env)
	pipelineManager.RegisterSingle(NewPipeline("test1", nil, nil, nil))

	_, err := pipelineManager.Run()
	assert.EqualError(t, err, "IMAGE environment variable is not set")
}

func TestPipelientManagerRunUnknownNamespace(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	env := &MockEnvironment{
		vars: map[string]string{
			"RUN_MODE": "root",
			"IMAGE":    "test-image",
		},
	}
	pipelineManager := NewPipelineManager(clientset, env)
	pipelineManager.RegisterSingle(NewPipeline("test1", nil, nil, nil))

	_, err := pipelineManager.Run()
	assert.Equal(t, errors.New("NAMESPACE environment variable is not set"), err)
}
