package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
)

type Result struct {
	city string
	min  float64
	max  float64
	avg  float64
}

type Info struct {
	count int64
	min   int64
	max   int64
	sum   int64
}

func main() {
	// fo, _ := os.Create("./output.txt")
	// w := bufio.NewWriter(fo)
	// w.WriteString(process())
	// fmt.Println(process())
	process()
}

func process() string {
	resultMap, err := readFile(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	var result = make([]Result, len(resultMap))

	var count int
	for city, calculated := range resultMap {
		result[count] = Result{
			city: city,
			min:  round(float64(calculated.min) / 10.0),
			max:  round(float64(calculated.max) / 10.0),
			avg:  round((float64(calculated.sum) / 10.0) / float64(calculated.count)),
		}
		count++
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].city < result[j].city
	})

	var stringsBuilder strings.Builder
	for _, i := range result {
		stringsBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", i.city, i.min, i.avg, i.max))
	}
	return stringsBuilder.String()[:stringsBuilder.Len()-2]
}

func readFile(path string) (resultMap map[string]Info, err error) {
	resultMap = map[string]Info{}
	resultStream := make(chan map[string]Info, 32)
	lineStream := make(chan []byte, 64)
	lineSize := 2048 * 2048

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			for chunk := range lineStream {
				readLine(string(chunk), resultStream)
			}
			wg.Done()
		}()
	}

	go func() {
		buf := make([]byte, lineSize)
		leftover := make([]byte, 0, lineSize)
		for {
			readTotal, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Fatal(err)
			}
			buf = buf[:readTotal]

			toSend := make([]byte, readTotal)
			copy(toSend, buf)

			lastNewLineIndex := bytes.LastIndex(buf, []byte{'\n'})

			toSend = append(leftover, buf[:lastNewLineIndex+1]...)
			leftover = make([]byte, len(buf[lastNewLineIndex+1:]))
			copy(leftover, buf[lastNewLineIndex+1:])

			lineStream <- toSend

		}
		close(lineStream)

		wg.Wait()
		close(resultStream)
	}()

	for t := range resultStream {
		for city, tempInfo := range t {
			if val, ok := resultMap[city]; ok {
				val.count += tempInfo.count
				val.sum += tempInfo.sum
				if tempInfo.min < val.min {
					val.min = tempInfo.min
				}

				if tempInfo.max > val.max {
					val.max = tempInfo.max
				}
				resultMap[city] = val
			} else {
				resultMap[city] = tempInfo
			}
		}
	}

	return resultMap, nil
}

func readLine(buffer string, resultStream chan<- map[string]Info) {
	res := make(map[string]Info)
	var init int
	var city string

	for ind, char := range buffer {
		switch char {
		case ';':
			city = buffer[init:ind]
			init = ind + 1
		case '\n':
			if (ind-init) > 1 && len(city) != 0 {
				temp := toInt(buffer[init:ind])
				init = ind + 1

				if val, ok := res[city]; ok {
					val.count++
					val.sum += temp
					if temp < val.min {
						val.min = temp
					}

					if temp > val.max {
						val.max = temp
					}
					res[city] = val
				} else {
					res[city] = Info{
						count: 1,
						min:   temp,
						max:   temp,
						sum:   temp,
					}
				}

				city = ""
			}
		}
	}
	resultStream <- res
}

func toInt(input string) (output int64) {
	var isNegative bool
	if input[0] == '-' {
		isNegative = true
		input = input[1:]
	}

	switch len(input) {
	case 3:
		output = int64(input[0]-'0')*10 + int64(input[2]-'0') - int64('0')*11
	case 4:
		output = int64(input[0]-'0')*100 + int64(input[1])*10 + int64(input[3]-'0') - (int64('0') * 111)
	}

	if isNegative {
		return -1 * output
	}
	return output
}

func round(x float64) float64 {
	return math.Round(x*10) / 10
}
