package impl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/Nutrymaco/data-processor/src/core"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Minio struct {
	bucketName string
	client     *minio.Client
}

func NewMinio(bucketName, endpoint, accessKeyID, secretAccessKey string) (*Minio, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	return &Minio{
		bucketName: bucketName,
		client:     client,
	}, nil
}

func (m *Minio) Read() (chan *core.Work, error) {
	objectsChan := m.client.ListObjects(context.Background(), m.bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})
	worksCh := make(chan *core.Work)
	counter := new(atomic.Int32)

	objects := []minio.ObjectInfo{}
	for object := range objectsChan {
		objects = append(objects, object)
	}

	for _, object := range objects {
		go func(obj minio.ObjectInfo, targetCount int) {
			reader, err := m.client.GetObject(context.Background(), m.bucketName, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				panic(err)
			}
			data := new(bytes.Buffer)
			_, err = io.Copy(data, reader)
			if err != nil {
				panic(err)
			}
			worksCh <- &core.Work{
				Data: data.Bytes(),
				Metadata: map[string]any{
					"name": obj.Key,
					"size": obj.Size,
				},
			}
			counter.Add(1)
			fmt.Println("read counter", counter.Load())
			fmt.Println("target", targetCount)
			if counter.Load() == int32(targetCount) {
				fmt.Println("send done from minio")
				worksCh <- nil
			}
		}(object, len(objects))
	}

	return worksCh, nil
}

func (m *Minio) Write(work *core.Work) error {
	fmt.Println("write")
	backetExists, err := m.client.BucketExists(context.Background(), m.bucketName)
	if err != nil {
		return err
	}
	if !backetExists {
		err = m.client.MakeBucket(context.Background(), m.bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}
	_, err = m.client.PutObject(context.Background(), m.bucketName, work.Metadata["name"].(string),
		bytes.NewReader(work.Data), work.Metadata["size"].(int64), minio.PutObjectOptions{},
	)
	if err != nil {
		return err
	}
	return nil
}

func (m *Minio) Done() {

}
