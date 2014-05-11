package shardkv

import "testing"
import "shardmaster"
import "math/rand"
import "strconv"
import "time"

func BenchmarkPut(b *testing.B) {
  smh, gids, ha, _, clean := setup("basic", false)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    ck.Put("a", "x")
  }
}

func BenchmarkGet(b *testing.B) {
  smh, gids, ha, _, clean := setup("basic", false)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  ck.Put("a", "x")
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    ck.Get("a")
  }
}

func BenchmarkPutHash(b *testing.B) {
  smh, gids, ha, _, clean := setup("basic", false)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    ck.PutHash("a", "b")
  }
}

func BenchmarkGetAfterJoin(b *testing.B) {
  for i := 0; i < b.N; i++ {
    b.StopTimer()
    smh, gids, ha, _, clean := setup("basic", false)

    mck := shardmaster.MakeClerk(smh)
    mck.Join(gids[0], ha[0])

    ck := MakeClerk(smh)
    keys := make([]string, 10)
    vals := make([]string, len(keys))
    for j := 0; j < len(keys); j++ {
      keys[j] = strconv.Itoa(rand.Int())
      vals[j] = strconv.Itoa(rand.Int())
      ck.Put(keys[j], vals[j])
    }
    b.StartTimer()
    mck.Join(gids[1], ha[1])
    time.Sleep(100 * time.Millisecond)
    v :=ck.Get(keys[5])
    b.StopTimer()
    if v != vals[5] {
      b.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
          1, keys[5], vals[5], v)
    }
    clean()
  }
}

// Reconfigs
// PutHash
// anything with outstanding RPC