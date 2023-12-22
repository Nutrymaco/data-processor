package core

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	source := NewArraySource([]*Work{
		NewStringWork("This is a sentence in which programm will count words"),
		NewStringWork("This is another sentence with words words"),
		NewStringWork("Two cows see each other"),
		NewStringWork("Two programmers write program that count words"),
	}, 1)
	target := NewArrayTarget()

	words := map[string]int{}
	lock := new(sync.Mutex)

	processor := NewProcessor(
		source,
		target,
		[]Action{
			// split text to words
			// 1->N
			NewAction(
				func(work *Work, out chan *Work) {
					for _, word := range strings.Split(work.ReadString(), " ") {
						out <- NewStringWork(word)
					}
				},
				func(out chan *Work) {

				},
			),
			// count words and produce answer
			// N->1
			NewAction(
				func(work *Work, out chan *Work) {
					word := work.ReadString()
					lock.Lock()
					count, exist := words[word]
					if !exist {
						words[word] = 1
					} else {
						words[word] = count + 1
					}
					lock.Unlock()
				},
				func(out chan *Work) {
					report := ""
					wordsArr := []struct {
						word  string
						count int
					}{}
					for word, count := range words {
						wordsArr = append(wordsArr, struct {
							word  string
							count int
						}{word, count})
					}
					sort.Slice(wordsArr, func(i, j int) bool {
						if wordsArr[i].count > wordsArr[j].count {
							return true
						} else if wordsArr[i].count == wordsArr[j].count {
							return wordsArr[i].word < wordsArr[j].word
						} else {
							return false
						}
					})
					for _, entry := range wordsArr {
						report += fmt.Sprintf("%s %d\n", entry.word, entry.count)
					}
					out <- NewStringWork(report)
				},
			),
		},
	)

	done, err := processor.Process()
	assert.Nil(t, err)
	<-done

	fmt.Println(target.GetArray())
	report := target.GetArray()[0].ReadString()
	fmt.Println("----------\nreport:")
	fmt.Println(report)
	assert.Equal(t,
		"words 4\n"+
			"This 2\n"+
			"Two 2\n"+
			"count 2\n"+
			"is 2\n"+
			"sentence 2\n"+
			"a 1\n"+
			"another 1\n"+
			"cows 1\n"+
			"each 1\n"+
			"in 1\n"+
			"other 1\n"+
			"program 1\n"+
			"programm 1\n"+
			"programmers 1\n"+
			"see 1\n"+
			"that 1\n"+
			"which 1\n"+
			"will 1\n"+
			"with 1\n"+
			"write 1\n",
		report)
}

func Test_MultipleTargets(t *testing.T) {
	source := NewArraySource([]*Work{
		NewStringWork("test").WithMetadata(map[string]string{"type": "1"}),
		NewStringWork("test2").WithMetadata(map[string]string{"type": "2"}),
		NewStringWork("test2").WithMetadata(map[string]string{"type": "2"}),
		NewStringWork("test").WithMetadata(map[string]string{"type": "1"}),
	}, 1)
	target1 := NewArrayTarget()
	target2 := NewArrayTarget()
	processor := NewProcessor(
		source,
		NewAggregatedTarget(
			TargetWithSelect(target1, func(metadata map[string]string) bool {
				return metadata["type"] == "1"
			}),
			TargetWithSelect(target2, func(metadata map[string]string) bool {
				return metadata["type"] == "2"
			}),
		),
		[]Action{},
	)
	done, err := processor.Process()
	assert.Nil(t, err)
	<-done
	fmt.Println("target1")
	for _, work := range target1.array {
		fmt.Print(work.ReadString())
		fmt.Println(" ", work.Metadata)
	}
	fmt.Println("target2")
	for _, work := range target2.array {
		fmt.Print(work.ReadString())
		fmt.Println(" ", work.Metadata)
	}
	fmt.Println()
	fmt.Println()
}

func Test_ChainOfPipelines(t *testing.T) {
	source1 := NewArraySource([]*Work{
		NewStringWork("data"),
		NewStringWork("data2"),
		NewStringWork("data2"),
		NewStringWork("data"),
	}, 1)
	pipe1 := NewChannelPipe()
	pipe2 := NewChannelPipe()
	processor := NewProcessor(
		source1,
		NewAggregatedTarget(
			TargetWithSelect(
				pipe1,
				func(metadata map[string]string) bool {
					return metadata["type"] == "1"
				}),
			TargetWithSelect(
				pipe2,
				func(metadata map[string]string) bool {
					return metadata["type"] == "2"
				},
			),
		),
		[]Action{
			NewAction(
				func(work *Work, out chan *Work) {
					str := work.ReadString()
					if len(str) == 4 {
						work.Metadata["type"] = "1"
					} else {
						work.Metadata["type"] = "2"
					}
					fmt.Println("[action] produce updated work", out)
					out <- work
					fmt.Println("[action] finish produce updated work")
				},
				func(out chan *Work) {

				},
			),
		},
	)
	target1 := NewArrayTarget()
	processor1 := NewProcessor(
		pipe1,
		target1,
		[]Action{},
	)
	target2 := NewArrayTarget()
	processor2 := NewProcessor(
		pipe2,
		target2,
		[]Action{},
	)

	done1, err := processor.Process()
	assert.Nil(t, err)
	done2, err := processor1.Process()
	assert.Nil(t, err)
	done3, err := processor2.Process()
	assert.Nil(t, err)
	<-done1
	<-done2
	<-done3

	target1Res := []string{}
	fmt.Println("target 1")
	for _, val := range target1.array {
		str := val.ReadString()
		fmt.Println(str, val.Metadata)
		target1Res = append(target1Res, str)
	}
	target2Res := []string{}
	fmt.Println("target 2")
	for _, val := range target2.array {
		str := val.ReadString()
		fmt.Println(str, val.Metadata)
		target2Res = append(target2Res, str)
	}
	assert.Equal(t, []string{"data", "data"}, target1Res)
	assert.Equal(t, []string{"data2", "data2"}, target2Res)
	fmt.Println()
	fmt.Println()
}

func Test_NtoMReducer(t *testing.T) {
	source := NewArraySource([]*Work{
		NewStringWork("val1"),
		NewStringWork("val2"),
		NewStringWork("val3"),
		NewStringWork("val4"),
		NewStringWork("val5"),
	}, 1)
	target := NewArrayTarget()

	accumulator := []*Work{}
	lock := new(sync.Mutex)
	processor := NewProcessor(
		source,
		target,
		[]Action{
			NewAction(
				func(work *Work, out chan *Work) {
					lock.Lock()
					accumulator = append(accumulator, work)
					if len(accumulator) == 2 {
						w1, w2 := accumulator[0], accumulator[1]
						accumulator = []*Work{}
						lock.Unlock()
						str1 := w1.ReadString()
						str2 := w2.ReadString()
						out <- NewStringWork(str1 + str2)
						return
					}
					lock.Unlock()
				},
				func(out chan *Work) {
					lock.Lock()
					str := accumulator[0].ReadString()
					out <- NewStringWork(str)
					lock.Unlock()
				},
			),
		},
	)
	done, err := processor.Process()
	assert.Nil(t, err)
	<-done
	targetRes := []string{}
	for _, work := range target.array {
		str := work.ReadString()
		targetRes = append(targetRes, str)
	}
	fmt.Println("result", targetRes)
	assert.Equal(t, []string{"val1val2", "val3val4", "val5"}, targetRes)
}

type ArraySource struct {
	array      []*Work
	timeoutSec int
}

func NewArraySource(array []*Work, timeoutSec int) *ArraySource {
	return &ArraySource{
		array:      array,
		timeoutSec: timeoutSec,
	}
}

func (s *ArraySource) Read() (chan *Work, error) {
	workCh := make(chan *Work)

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

type ArrayTarget struct {
	array []*Work
	lock  *sync.Mutex
}

func NewArrayTarget() *ArrayTarget {
	return &ArrayTarget{
		array: []*Work{},
		lock:  new(sync.Mutex),
	}
}

func (t *ArrayTarget) Write(work *Work) error {
	t.lock.Lock()
	t.array = append(t.array, work)
	t.lock.Unlock()
	fmt.Println("[array target] write work to target")
	return nil
}

func (t *ArrayTarget) Done() {

}

func (t *ArrayTarget) GetArray() []*Work {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.array
}

type GenericAction struct {
	do   func(work *Work, out chan *Work)
	done func(out chan *Work)
}

func NewAction(do func(work *Work, out chan *Work), done func(out chan *Work)) *GenericAction {
	return &GenericAction{
		do:   do,
		done: done,
	}
}

func (a *GenericAction) Do(work *Work, out chan *Work) {
	a.do(work, out)
}
func (a *GenericAction) Done(out chan *Work) {
	a.done(out)
}

type ChannelPipe chan *Work

func NewChannelPipe() *ChannelPipe {
	pipe := ChannelPipe(make(chan *Work))
	return &pipe
}

func (p *ChannelPipe) Read() (chan *Work, error) {
	fmt.Print("[pipe] Read()")
	return *p, nil
}

func (p *ChannelPipe) Write(work *Work) error {
	fmt.Println("[pipe] Write(work)", work)
	*p <- work
	return nil
}

func (p *ChannelPipe) Done() {
	fmt.Println("[pipe] Done()")
	*p <- nil
}
