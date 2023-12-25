package core

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_CountWords(t *testing.T) {
	source := NewArraySource([]any{
		"This is a sentence in which programm will count words",
		"This is another sentence with words words",
		"Two cows see each other",
		"Two programmers write program that count words",
	}, 1)
	target := NewArrayTarget[map[string]int]()

	pipeline := NewPipeline(
		"Count words",
		source,
		target,
		[]Action{
			// split text to words
			// 1->N
			NewFlatMapAction(
				func(work string, out func(string)) {
					for _, word := range strings.Split(work, " ") {
						out(word)
					}
				},
			),
			// count words and produce answer
			// N->1
			NewReduceAction(
				map[string]int{},
				func(report map[string]int, word string) map[string]int {
					count, exist := report[word]
					if !exist {
						report[word] = 1
					} else {
						report[word] = count + 1
					}
					return report
				},
			),
		},
	)

	done, err := pipeline.Run()
	assert.Nil(t, err)
	<-done

	fmt.Println(target.GetArray())
	report := target.GetArray()[0]
	assert.Equal(t,
		map[string]int{
			"words":       4,
			"This":        2,
			"Two":         2,
			"count":       2,
			"is":          2,
			"sentence":    2,
			"a":           1,
			"another":     1,
			"cows":        1,
			"each":        1,
			"in":          1,
			"other":       1,
			"program":     1,
			"programm":    1,
			"programmers": 1,
			"see":         1,
			"that":        1,
			"which":       1,
			"will":        1,
			"with":        1,
			"write":       1,
		},
		report)
}

func Test_ChainOfPipelines(t *testing.T) {
	source1 := NewArraySource([]string{"data", "data2", "data2", "data"}, 1)
	pipe1 := NewChannelPipe()
	pipe2 := NewChannelPipe()
	pipeline := NewPipeline(
		"Common",
		source1,
		NewAggregatedTarget(
			TargetWithSelect(
				pipe1,
				func(work string) bool {
					return len(work) == 4
				}),
			TargetWithSelect(
				pipe2,
				func(work string) bool {
					return len(work) == 5
				},
			),
		),
		[]Action{},
	)
	target1 := NewArrayTarget[string]()
	pipeline1 := NewPipeline(
		"Type=1",
		pipe1,
		target1,
		[]Action{},
	)
	target2 := NewArrayTarget[string]()
	pipeline2 := NewPipeline(
		"Type=2",
		pipe2,
		target2,
		[]Action{},
	)

	done1, err := pipeline.Run()
	assert.Nil(t, err)
	done2, err := pipeline1.Run()
	assert.Nil(t, err)
	done3, err := pipeline2.Run()
	assert.Nil(t, err)
	<-done1
	<-done2
	<-done3

	target1Res := []string{}
	fmt.Println("target 1")
	for _, val := range target1.array {
		target1Res = append(target1Res, val)
	}
	target2Res := []string{}
	fmt.Println("target 2")
	for _, val := range target2.array {
		str := val
		fmt.Println(str)
		target2Res = append(target2Res, str)
	}
	assert.Equal(t, []string{"data", "data"}, target1Res)
	assert.Equal(t, []string{"data2", "data2"}, target2Res)
	fmt.Println()
	fmt.Println()
}

func Test_NtoMReducer(t *testing.T) {
	source := NewArraySource([]string{"val1", "val2", "val3", "val4", "val5"}, 1)
	target := NewArrayTarget[string]()

	accumulator := []string{}
	lock := new(sync.Mutex)
	pipeline := NewPipeline(
		"Combining values",
		source,
		target,
		[]Action{
			NewAction(
				func(work string, out func(string)) {
					lock.Lock()
					accumulator = append(accumulator, work)
					if len(accumulator) == 2 {
						w1, w2 := accumulator[0], accumulator[1]
						accumulator = []string{}
						lock.Unlock()
						out(w1 + w2)
						return
					}
					lock.Unlock()
				},
				func(out func(string)) {
					lock.Lock()
					str := accumulator[0]
					out(str)
					lock.Unlock()
				},
			),
		},
	)
	done, err := pipeline.Run()
	assert.Nil(t, err)
	<-done
	targetRes := []string{}
	for _, work := range target.array {
		str := work
		targetRes = append(targetRes, str)
	}
	fmt.Println("result", targetRes)
	assert.Equal(t, []string{"val1val2", "val3val4", "val5"}, targetRes)
}

func Test_ErrorInSourceLeadToError(t *testing.T) {
	source := NewGenericSource(
		func() (<-chan any, error) {
			return nil, errors.New("error in source")
		},
	)
	pipeline := NewPipeline("Error in source", source, nil, nil)
	ch, err := pipeline.Run()
	assert.NotNil(t, err)
	assert.Nil(t, ch)
}

type ArraySource[T any] struct {
	array      []T
	timeoutSec int
}

func NewArraySource[T any](array []T, timeoutSec int) *ArraySource[T] {
	return &ArraySource[T]{
		array:      array,
		timeoutSec: timeoutSec,
	}
}

func (s *ArraySource[T]) Read() (<-chan any, error) {
	workCh := make(chan any)

	go func() {
		for _, val := range s.array {
			workCh <- val
			time.Sleep(time.Duration(s.timeoutSec * int(time.Second)))
		}
		fmt.Println("[array source] produce nil")
		workCh <- nil
	}()
	return workCh, nil
}

type ArrayTarget[T any] struct {
	array []T
	lock  *sync.Mutex
}

func NewArrayTarget[T any]() *ArrayTarget[T] {
	return &ArrayTarget[T]{
		array: []T{},
		lock:  new(sync.Mutex),
	}
}

func (t *ArrayTarget[T]) Write(work any) {
	t.lock.Lock()
	t.array = append(t.array, work.(T))
	t.lock.Unlock()
	fmt.Println("[array target] write work to target")
}

func (t *ArrayTarget[T]) Done() {

}

func (t *ArrayTarget[T]) GetArray() []T {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.array
}

type ChannelPipe chan any

func NewChannelPipe() *ChannelPipe {
	pipe := ChannelPipe(make(chan any))
	return &pipe
}

func (p *ChannelPipe) Read() (<-chan any, error) {
	fmt.Print("[pipe] Read()")
	return *p, nil
}

func (p *ChannelPipe) Write(work any) {
	fmt.Println("[pipe] Write(work)", work)
	*p <- work
}

func (p *ChannelPipe) Done() {
	fmt.Println("[pipe] Done()")
	*p <- nil
}

type DiscardTarget struct{}

func NewDiscardTarget() *DiscardTarget {
	return &DiscardTarget{}
}
func (t *DiscardTarget) Write(work any) error {
	return nil
}
func (t *DiscardTarget) Done() {

}

type GenericSource struct {
	read func() (<-chan any, error)
}

func NewGenericSource(read func() (<-chan any, error)) *GenericSource {
	return &GenericSource{
		read: read,
	}
}

func (s *GenericSource) Read() (<-chan any, error) {
	return s.read()
}

type GenericTarget struct {
	write func(work any)
	done  func()
}

func NewGenericTarget(write func(work any), done func()) *GenericTarget {
	return &GenericTarget{
		write: write,
		done:  done,
	}
}

func (t *GenericTarget) Write(work any) {
	t.write(work)
}

func (t *GenericTarget) Done() {
	t.done()
}

type GenericAction[I, O any] struct {
	do   func(work I, out func(O))
	done func(out func(O))
}

func NewAction[I, O any](do func(work I, out func(O)), done func(out func(O))) *GenericAction[I, O] {
	return &GenericAction[I, O]{
		do:   do,
		done: done,
	}
}

func (a *GenericAction[I, O]) Do(work any, out chan<- any) {
	a.do(work.(I), func(o O) {
		out <- o
	})
}
func (a *GenericAction[I, O]) Done(out chan<- any) {
	a.done(func(o O) {
		out <- o
	})
}
