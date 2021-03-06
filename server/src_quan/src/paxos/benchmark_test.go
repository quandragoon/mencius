package paxos

import "testing"
import "time"

func ndecidedBenchmark(b *testing.B, pxa []*Paxos, seq int) int {
  count := 0
  var v interface{}
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      decided, v1 := pxa[i].Status(seq)
      if decided {
        if count > 0 && v != v1 {
          b.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
            seq, i, v, v1)
        }
        count++
        v = v1
      }
    }
  }
  return count
}

func waitnBenchmark(b *testing.B, pxa[]*Paxos, seq int, wanted int) {
  to := time.Millisecond
  //for iters := 0; iters < 100; iters++ {
  for {
    if ndecidedBenchmark(b, pxa, seq) >= wanted {
      break
    }
    time.Sleep(to)
    // if to < time.Second {
    //   to *= 2
    // }
  }
  nd := ndecidedBenchmark(b, pxa, seq)
  if nd < wanted {
    b.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
}

func waitmajorityBenchmark(b *testing.B, pxa[]*Paxos, seq int) {
  waitnBenchmark(b, pxa, seq, (len(pxa) / 2) + 1)
}

// func BenchmarkRoundtrip(b *testing.B) {
//   for i := 0; i < b.N; i++ {
//     b.StopTimer()
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = port("basic", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//     }
//     b.StartTimer()
//     pxa[0].Start(0, "hello")
//     waitnBenchmark(b, pxa, 0, npaxos)
//     b.StopTimer()
//     cleanup(pxa)
//   }
// }

func BenchmarkConflictThree(b *testing.B) {
  const npaxos = 7
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("basic", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }
  b.StartTimer()
  for j := 0; j < npaxos; j++ {
    pxa[j].Start(0, j*100)
  }
  b.ResetTimer()
  for i := 1; i < b.N; i++ {
    for j := 0; j < npaxos; j++ {
      pxa[j].Start(i, j*100)
    }
    waitmajorityBenchmark(b, pxa, i)
  }
}

// func BenchmarkConflictFive(b *testing.B) {
//   const npaxos = 5
//   var pxa []*Paxos = make([]*Paxos, npaxos)
//   var pxh []string = make([]string, npaxos)
//   defer cleanup(pxa)

//   for i := 0; i < npaxos; i++ {
//     pxh[i] = port("basic", i)
//   }
//   for i := 0; i < npaxos; i++ {
//     pxa[i] = Make(pxh, i, nil)
//   }
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     for j := 0; j < npaxos; j++ {
//       pxa[j].Start(0, j*100)
//     }
//     waitmajorityBenchmark(b, pxa, 0)
//   }
// }

// func BenchmarkConflictTen(b *testing.B) {
//   const npaxos = 10
//   var pxa []*Paxos = make([]*Paxos, npaxos)
//   var pxh []string = make([]string, npaxos)
//   defer cleanup(pxa)

//   for i := 0; i < npaxos; i++ {
//     pxh[i] = port("basic", i)
//   }
//   for i := 0; i < npaxos; i++ {
//     pxa[i] = Make(pxh, i, nil)
//   }
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     for j := 0; j < npaxos; j++ {
//       pxa[j].Start(0, j*100)
//     }
//     waitmajorityBenchmark(b, pxa, 0)
//   }
// }