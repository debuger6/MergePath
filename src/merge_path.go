package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

func main() {
	var xn, yn int
	var t int
	flag.IntVar(&xn, "x", 10, "the number of array X")
	flag.IntVar(&yn, "y", 10, "the number of array Y")
	flag.IntVar(&t, "t", 1, "the number of threads")
	flag.Parse()

	A, B := generateAB(xn, yn)
	start := time.Now().UnixNano()
	C1 := seriesMergeSort(A, B)
	seriesCost := time.Now().UnixNano() - start
	fmt.Println("series merge cost: ", seriesCost, "ns")

	start = time.Now().UnixNano()
	C2 := mergePath(A, B, t)
	mergePathCost := time.Now().UnixNano() - start
	fmt.Println("merge path cost: ", mergePathCost, "ns")
	fmt.Println("speedup ratio: ", float64(seriesCost)/float64(mergePathCost))

	// verify
	if len(C1) != len(C2) {
		panic("wrong answer(len(c1) != len(C2))")
	}
	for i := 0; i < len(C1); i++ {
		if C1[i] != C2[i] {
			panic("wrong answer(C1 != C2)")
		}
	}
}

func generateAB(x, y int) ([]int, []int) {
	rand.Seed(time.Now().Unix())
	A, B := make([]int, x), make([]int, y)
	for i := 0; i < x; i++ {
		A[i] = rand.Intn(10*x)
	}
	for i := 0; i < y; i++ {
		B[i] = rand.Intn(10*y)
	}
	sort.Ints(A)
	sort.Ints(B)
	return A, B
}

func seriesMergeSort(A, B []int) []int {
	C := make([]int, 0, len(A)+len(B))
	var i, j int
	for i<len(A) && j<len(B) {
		if A[i] < B[j] {
			C = append(C, A[i])
			i++
		} else {
			C = append(C, B[j])
			j++
		}
	}
	if i<len(A) {
		C = append(C, A[i:]...)
	}
	if j<len(B) {
		C = append(C, B[j:]...)
	}
	return C
}

func findSplitPoints(A, B []int, numberOfCore int) []int {
	// 用一维数组表示分割点
	splitPoints := make([]int, numberOfCore*2+2)
	// 初始化终点
	splitPoints[numberOfCore*2], splitPoints[numberOfCore*2+1] = len(A), len(B)

	var wg sync.WaitGroup
	for core := 0; core < numberOfCore; core++ {
		wg.Add(1)
		// 每个线程分别找自己的分割点（起点）
		go func(coreNum int) {
			var maxX, minX, x, y int
			// 当前线程负责片段在Merge Path上的偏移
			combineIndex := coreNum * (len(A)+len(B))/numberOfCore
			if combineIndex > len(A) {
				maxX = len(A)
			} else {
				maxX = combineIndex
			}
			for {
				// 二分查找x的位置，找到x也就找到了y，因为x+y=combineIndex
				x = (maxX+minX)/2
				y = combineIndex - x
				if y > len(B) {
					y = len(B)
					x = combineIndex-y
				}

				var bigger bool
				// 比较当前A和B的上一个值
				if y == 0 || x == len(A) || A[x] > B[y-1] {
					// 假想下标小于0的值为负无穷，下标大于上界的值为正无穷
					bigger = true
				}
				if bigger {
					var smaller bool
					// 继续比较A的上一个值和当前B
					if x == 0 || y == len(B) || A[x-1] <= B[y]{
						smaller = true
					}
					if smaller {
						// 到这一步满足A[x]>B[y-1]且B[y]>=A[x-1]，说明x,y是分割点
						splitPoints[2*coreNum] = x
						splitPoints[2*coreNum+1] = y
						break
					} else {
						maxX = x-1
					}
				} else {
					minX = x+1
				}
			}
			wg.Done()
		}(core)
	}
	wg.Wait()
	return splitPoints
}

func mergePath(A, B []int, numberOfCore int) []int {
	C := make([]int, len(A)+len(B))
	splitPoints := findSplitPoints(A, B, numberOfCore)
	var wg sync.WaitGroup
	for i := 0; i < numberOfCore; i++ {
		wg.Add(1)
		go func(coreNum int) {
			// 获取片段的起点和终点，每个线程负责自己的片段，所以不会出现内存并发安全问题
			startX := splitPoints[2*coreNum]
			startY := splitPoints[2*coreNum+1]
			endX := splitPoints[2*(coreNum+1)]
			endY := splitPoints[2*(coreNum+1)+1]
			j, k := startX, startY
			index := coreNum * (len(A)+len(B))/numberOfCore
			for j<endX&&k<endY {
				if A[j] < B[k] {
					C[index] = A[j]
					j++
				} else {
					C[index] = B[k]
					k++
				}
				index++
			}
			for j < endX {
				C[index] = A[j]
				j++
				index++
			}
			for k < endY {
				C[index] = B[k]
				k++
				index++
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return C
}

