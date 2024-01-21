package core

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Environment interface {
	Get(string) (string, bool)
}

type PipelineManager struct {
	clientset       kubernetes.Interface
	singlePipelines []*Pipeline
	multiPipelines  map[*Pipeline]int
	env             Environment
}

func NewPipelineManager(clientset kubernetes.Interface, env Environment) *PipelineManager {
	return &PipelineManager{
		clientset:       clientset,
		singlePipelines: make([]*Pipeline, 0),
		multiPipelines:  make(map[*Pipeline]int),
		env:             env,
	}
}
func (pr *PipelineManager) RegisterSingle(pipeline *Pipeline) {
	pr.singlePipelines = append(pr.singlePipelines, pipeline)
}

func (pr *PipelineManager) RegisterMulti(pipeline *Pipeline, count int) {
	pr.multiPipelines[pipeline] = count
}

func (pr *PipelineManager) Run() (<-chan struct{}, error) {
	runMode, _ := pr.env.Get("RUN_MODE")
	switch runMode {
	case "root":
		err := pr.runAsRoot()
		if err != nil {
			return nil, err
		}
		done := make(chan struct{}, 1)
		done <- struct{}{}
		return done, nil
	case "pipeline":
		pipelineName, _ := pr.env.Get("PIPELINE")
		for _, pipeline := range pr.singlePipelines {
			if pipeline.name == pipelineName {
				return pipeline.Run()
			}
		}
		for pipeline := range pr.multiPipelines {
			if pipeline.name == pipelineName {
				return pipeline.Run()
			}
		}
		return nil, errors.New("not found pipeline to run")
	default:
		return nil, errors.New("unknown mode")
	}
}

func (pr *PipelineManager) runAsRoot() error {
	log.Info().Msg("Run as root")
	for _, pipeline := range pr.singlePipelines {
		if err := pr.runPipeline(pipeline, 1); err != nil {
			return err
		}
	}

	for pipeline, count := range pr.multiPipelines {
		if err := pr.runPipeline(pipeline, count); err != nil {
			return err
		}
	}

	return nil
}

func (pr *PipelineManager) runPipeline(pipeline *Pipeline, replica int) error {
	log.Info().Str("pipeline_name", pipeline.name).Msg("Run pipeline")
	image, exists := pr.env.Get("IMAGE")
	if !exists {
		return errors.New("IMAGE environment variable is not set")
	}
	namespace, exists := pr.env.Get("NAMESPACE")
	if !exists {
		return errors.New("NAMESPACE environment variable is not set")
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pipeline-" + pipeline.name,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(replica),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pipeline",
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "pipeline",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "pipeline",
							Image: image,
							Ports: []v1.ContainerPort{
								{
									ContainerPort: 8080,
								},
							},
							Env: []v1.EnvVar{
								{
									Name:  "PIPELINE",
									Value: pipeline.name,
								},
								{
									Name:  "RUN_MODE",
									Value: "pipeline",
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := pr.clientset.AppsV1().Deployments(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		log.Err(err).Str("deployment_name", deployment.GetName()).Msg("Error while create deployment")
	}
	return err
}

func int32Ptr(i int) *int32 {
	v := int32(i)
	return &v
}
