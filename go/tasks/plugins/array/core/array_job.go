package core

type ArrayJob struct {
	Parallelism     int64
	Size            int64
	MinSuccesses    int64
	MinSuccessRatio float64
}

func (a ArrayJob) GetParallelism() int64 {
	return a.Parallelism
}

func (a ArrayJob) GetSize() int64 {
	return a.Size
}

func (a ArrayJob) GetMinSuccesses() int64 {
	return a.MinSuccesses
}

func (a ArrayJob) GetMinSuccessRatio() float64 {
	return a.MinSuccessRatio
}
