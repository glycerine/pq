package pq

import (
	"container/heap"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	cv "github.com/glycerine/goconvey/convey"
	tf "github.com/glycerine/tmframe"
	"os"
	"testing"
	"time"
)

func Test001PqSorts(t *testing.T) {

	cv.Convey("a priority queue with a timestamp key should return items in chronological seequence", t, func() {

		n := 100
		frames, _, _ := GenTestFrames(n, nil)

		// Create a priority queue, put the items in it, and
		// establish the priority queue (heap) invariants.
		pq := NewPriorityQueue()
		m := n - 1
		for i := range frames {
			//k := cryptoRandNonNegInt() % n
			pq.Add(frames[m-i])
		}

		// modify a priority with Update:
		// a) first add a duplicate of 0
		pqe, err := pq.Add(frames[0])
		cv.So(err, cv.ShouldBeNil)
		// b) then update the value of that pqe
		pq.Update(pqe, frames[n-1])

		// Take the items out; they arrive in ascending order
		fmt.Println()
		var prevTm time.Time = pq.First().OrderBy
		for pq.Len() > 0 {
			item := heap.Pop(pq).(*Pqe)
			cv.So(!item.OrderBy.Before(prevTm), cv.ShouldBeTrue)
			prevTm = item.OrderBy
			fmt.Printf("%v: %v\n", item.OrderBy, item.Val)
		}
	})
}

// generate n test Frames, with 4 different frame types, and randomly varying sizes
// if outpath is non-nill, write to that file.
func GenTestFrames(n int, outpath *string) (frames []*tf.Frame, tms []time.Time, by []byte) {

	t0, err := time.Parse(time.RFC3339, "2016-02-16T00:00:00Z")
	panicOn(err)
	t0 = t0.UTC()

	var f0 *tf.Frame
	for i := 0; i < n; i++ {
		t := t0.Add(time.Second * time.Duration(i))
		tms = append(tms, t)
		switch i % 3 {
		case 0:
			// generate a random length message payload
			m := cryptoRandNonNegInt() % 254
			data := make([]byte, m)
			for j := 0; j < m; j++ {
				data[j] = byte(j)
			}
			f0, err = tf.NewFrame(t, tf.EvMsgpKafka, 0, 0, data)
			panicOn(err)
		case 1:
			f0, err = tf.NewFrame(t, tf.EvZero, 0, 0, nil)
			panicOn(err)
		case 2:
			f0, err = tf.NewFrame(t, tf.EvTwo64, float64(i), int64(i), nil)
			panicOn(err)
		case 3:
			f0, err = tf.NewFrame(t, tf.EvOneFloat64, float64(i), 0, nil)
			panicOn(err)
		}
		frames = append(frames, f0)
		b0, err := f0.Marshal(nil)
		panicOn(err)
		by = append(by, b0...)
	}

	if outpath != nil {
		f, err := os.Create(*outpath)
		panicOn(err)
		_, err = f.Write(by)
		panicOn(err)
		f.Close()
	}

	return
}

func cryptoRandNonNegInt() int {
	b := make([]byte, 8)
	_, err := cryptorand.Read(b)
	panicOn(err)
	r := int(binary.LittleEndian.Uint64(b))
	if r < 0 {
		return -r
	}
	return r
}
