package main

import "testing"
import "math/rand"
//import "fmt"

// Benchmarking disk writes vs memory writes

var result string


func BenchmarkMemoryWrites(b *testing.B) {
  keyvalues := make(map[int]map[int]string)
  keyvalues[0] = make(map[int]string)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    big := make([]byte, 10)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    keyvalues[0][i * 10] = string(big)
  }
}

func BenchmarkDiskWrites(b *testing.B) {
  basePath := "/tmp/Data"
  d := new(DiskIO)
  d.BasePath = basePath
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    big := make([]byte, 10)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    d.write(0, string(i * 10), string(big))
  }
}

func BenchmarkDiskWritesIOUtil(b *testing.B) {
  basePath := "/tmp/Data"
  d := new(DiskIO)
  d.BasePath = basePath
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    big := make([]byte, 10)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    d.writeIOUtil(0, string(i * 10), string(big))
  }
}

func BenchmarkMemoryReads(b *testing.B) {
  keyvalues := make(map[int]map[int]string)
  keyvalues[0] = make(map[int]string)
  for i := 0; i < 50; i++ {
    big := make([]byte, 10)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    keyvalues[0][i * 10] = string(big)
  }
  b.ResetTimer()
  answer := ""
  for i := 0; i < b.N; i++ {
    temp, _ := keyvalues[0][(i % 50) * 10]
    answer += temp
  }
  result = answer
}

func BenchmarkDiskReads(b *testing.B) {
  basePath := "/tmp/Data"
  d := new(DiskIO)
  d.BasePath = basePath
  for i := 0; i < 50; i++ {
    big := make([]byte, 10)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    d.write(0, string(i * 10), string(big))
  }
  b.ResetTimer()
  answer := ""
  for i := 0; i < b.N; i++ {
    temp, _ := d.read(0, string((i % 50) * 10))
    answer += temp
  }
  result = answer
}