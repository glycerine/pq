package pq

import (
	tm "github.com/glycerine/tmframe"
	"io"
)

// FrameRingBuf:
//
//  a fixed-size circular ring buffer of *tm.Frame
//
type FrameRingBuf struct {
	A        []*tm.Frame
	N        int // MaxView, the total size of A, whether or not in use.
	Beg      int // start of in-use data in A
	Readable int // number of pointers available in A (in use)
}

// NewFrameRingBuf returns a new FrameRingBuf; it
// will allocate internally a slice of size maxSize
func NewFrameRingBuf(maxSize int) *FrameRingBuf {
	n := maxSize
	r := &FrameRingBuf{
		N:        n,
		Beg:      0,
		Readable: 0,
	}
	r.A = make([]*tm.Frame, n, n)

	return r
}

// TwoContig returns all readable pointers, but in two separate slices,
// to avoid copying. The two slices are from the same buffer, but
// are not contiguous. Either or both may be empty slices.
func (b *FrameRingBuf) TwoContig(makeCopy bool) (first []*tm.Frame, second []*tm.Frame) {

	extent := b.Beg + b.Readable
	if extent <= b.N {
		// we fit contiguously in this buffer without wrapping to the other.
		// Let second stay an empty slice.
		return b.A[b.Beg:(b.Beg + b.Readable)], second
	}

	return b.A[b.Beg:(b.Beg + b.Readable)], b.A[0:(extent % b.N)]
}

// RingReadFrames reads the next len(p) *tm.Frame
// pointers from the internal ring into p, or until the ring
// is drained. The return value n is the number of pointers
// read. If the buffer has no data to return, err is io.EOF
// (unless len(p) is zero); otherwise it is nil.
func (b *FrameRingBuf) RingReadFrames(p []*tm.Frame) (n int, err error) {
	return b.readAndMaybeAdvance(p, true)
}

// ReadWithoutAdvance(): if you want to Read the data and leave
// it in the buffer, so as to peek ahead for example.
func (b *FrameRingBuf) RingReadWithoutAdvance(p []*tm.Frame) (n int, err error) {
	return b.readAndMaybeAdvance(p, false)
}

func (b *FrameRingBuf) readAndMaybeAdvance(p []*tm.Frame, doAdvance bool) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.Readable == 0 {
		return 0, io.EOF
	}
	extent := b.Beg + b.Readable
	if extent <= b.N {
		n += copy(p, b.A[b.Beg:extent])
	} else {
		n += copy(p, b.A[b.Beg:b.N])
		if n < len(p) {
			n += copy(p[n:], b.A[0:(extent%b.N)])
		}
	}
	if doAdvance {
		b.Advance(n)
	}
	return
}

//
// RingWriteFrames writes len(p) *tm.Frame values from p to
// the underlying data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// RingWriteFrames must return a non-nil error if it returns n < len(p).
//
func (b *FrameRingBuf) RingWriteFrames(p []*tm.Frame) (n int, err error) {
	for {
		if len(p) == 0 {
			// nothing (left) to copy in; notice we shorten our
			// local copy p (below) as we read from it.
			return
		}

		writeCapacity := b.N - b.Readable
		if writeCapacity <= 0 {
			// we are all full up already.
			return n, io.ErrShortWrite
		}
		if len(p) > writeCapacity {
			err = io.ErrShortWrite
			// leave err set and
			// keep going, write what we can.
		}

		writeStart := (b.Beg + b.Readable) % b.N

		upperLim := intMin(writeStart+writeCapacity, b.N)

		k := copy(b.A[writeStart:upperLim], p)

		n += k
		b.Readable += k
		p = p[k:]

		// we can fill from b.A[0:something] from
		// p's remainder, so loop
	}
}

// Reset quickly forgets any data stored in the ring buffer. The
// data is still there, but the ring buffer will ignore it and
// overwrite those buffers as new data comes in.
func (b *FrameRingBuf) Reset() {
	b.Beg = 0
	b.Readable = 0
}

// Advance(): non-standard, but better than Next(),
// because we don't have to unwrap our buffer and pay the cpu time
// for the copy that unwrapping may need.
// Useful in conjuction/after ReadWithoutAdvance() above.
func (b *FrameRingBuf) Advance(n int) {
	if n <= 0 {
		return
	}
	if n > b.Readable {
		n = b.Readable
	}
	b.Readable -= n
	b.Beg = (b.Beg + n) % b.N
}

// Adopt: for efficiency's sake, (possibly) take ownership of
// already allocated slice offered in me.
//
// If me is large we will adopt it, and we will potentially then
// write to the me buffer.
// If we already have a bigger buffer, copy me into the existing
// buffer instead.
func (b *FrameRingBuf) Adopt(me []*tm.Frame) {
	n := len(me)
	if n > b.N {
		b.A = me
		b.N = n
		b.Beg = 0
		b.Readable = n
	} else {
		// we already have a larger buffer, reuse it.
		copy(b.A, me)
		b.Beg = 0
		b.Readable = n
	}
}

/*
func intMax(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func intMin(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
*/
