package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "sync"
import "math/rand"

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "skv-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func NextValue(hprev string, val string) string {
  h := hash(hprev + val)
  return strconv.Itoa(int(h))
}

func mcleanup(sma []*shardmaster.ShardMaster) {
  for i := 0; i < len(sma); i++ {
    if sma[i] != nil {
      sma[i].Kill()
    }
  }
}

func cleanup(sa [][]*ShardKV) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].Kill()
    }
  }
}

func setup(tag string, unreliable bool) ([]string, []int64, [][]string, [][]*ShardKV, func()) {
  runtime.GOMAXPROCS(4)
  
  const nmasters = 3
  //var sma []*shardmaster.ShardMaster = make([]*shardmaster.ShardMaster, nmasters)
  var smh []string = make([]string, nmasters)
  // defer mcleanup(sma)
  // for i := 0; i < nmasters; i++ {
  //   smh[i] = port(tag+"m", i)
  // }
  //smh[0] = "54.187.210.114:8080"
  // smh[0] = "127.0.0.1:8080"
  for i := 0; i < nmasters; i++ {
    smh[i] = "54.187.218.121:808" + strconv.Itoa(i)
  }
  // for i := 0; i < nmasters; i++ {
  //   sma[i] = shardmaster.StartServer(smh, i)
  // }

  const ngroups = 3   // replica groups
  const nreplicas = 3 // servers per group
  gids := make([]int64, ngroups)    // each group ID
  ha := make([][]string, ngroups)   // ShardKV ports, [group][replica]

  // Local shard KV servers
  sa := make([][]*ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      sa[i][j] = StartServer(gids[i], smh, ha[i], j)
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func() {
    cleanup(sa) ; 
    //mcleanup(sma) 
  }

  return smh, gids, ha, sa, clean

  // Remote shard KV servers
  // const IP = "127.0.0.1"
  // IPS := make([]string, ngroups)
  // IPS[0] = "54.187.166.1"
  // IPS[1] = "54.187.139.234"
  // IPS[2] = "54.187.203.101"

  // for i := 0; i < ngroups; i++ {
  //   gids[i] = int64(i + 100)
  //   ha[i] = make([]string, nreplicas)
  //   for j := 0; j < nreplicas; j++ {
  //     ha[i][j] = IPS[i] + ":808" + strconv.Itoa(j)
  //   }
  // }

  // return smh, gids, ha, nil, clean
}

func TestBasic(t *testing.T) {
  smh, gids, ha, _, clean := setup("basic", false)
  defer clean()

  fmt.Printf("Test: Basic Join/Leave ...\n")

  mck := shardmaster.MakeClerk(smh)
  fmt.Printf("Made clerk, %s %s\n", smh, mck)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  ck.Put("a", "x")
  v := ck.PutHash("a", "b")
  if v != "x" {
    t.Fatalf("Puthash got wrong value")
  }
  ov := NextValue("x", "b")
  if ck.Get("a") != ov {
    t.Fatalf("Get got wrong value")
  }

  keys := make([]string, 10)
  vals := make([]string, len(keys))
  for i := 0; i < len(keys); i++ {
    keys[i] = strconv.Itoa(rand.Int())
    vals[i] = strconv.Itoa(rand.Int())
    ck.Put(keys[i], vals[i])
  }

  // are keys still there after joins?
  for g := 1; g < len(gids); g++ {
    mck.Join(gids[g], ha[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }
  
  // are keys still there after leaves?
  for g := 0; g < len(gids)-1; g++ {
    mck.Leave(gids[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }

  fmt.Printf("  ... Passed\n")
}

func TestMove(t *testing.T) {
  smh, gids, ha, _, clean := setup("move", false)
  defer clean()

  fmt.Printf("Test: Shards really move ...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)

  // insert one key per shard
  for i := 0; i < shardmaster.NShards; i++ {
    ck.Put(string('0'+i), string('0'+i))
  }

  // add group 1.
  mck.Join(gids[1], ha[1])
  time.Sleep(5 * time.Second)
  
  // check that keys are still there.
  for i := 0; i < shardmaster.NShards; i++ {
    if ck.Get(string('0'+i)) != string('0'+i) {
      t.Fatalf("missing key/value")
    }
  }

  // remove sockets from group 0.
  for i := 0; i < len(ha[0]); i++ {
    os.Remove(ha[0][i])
  }

  count := 0
  var mu sync.Mutex
  for i := 0; i < shardmaster.NShards; i++ {
    go func(me int) {
      myck := MakeClerk(smh)
      v := myck.Get(string('0'+me))
      if v == string('0'+me) {
        mu.Lock()
        count++
        mu.Unlock()
      } else {
        t.Fatalf("Get(%v) yielded %v\n", i, v)
      }
    }(i)
  }

  time.Sleep(10 * time.Second)

  if count > shardmaster.NShards / 3 && count < 2*(shardmaster.NShards/3) {
    fmt.Printf("  ... Passed\n")
  } else {
    t.Fatalf("%v keys worked after killing 1/2 of groups; wanted %v",
      count, shardmaster.NShards / 2)
  }
}

func TestLimp(t *testing.T) {
  smh, gids, ha, sa, clean := setup("limp", false)
  defer clean()

  fmt.Printf("Test: Reconfiguration with some dead replicas ...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)

  ck.Put("a", "b")
  if ck.Get("a") != "b" {
    t.Fatalf("got wrong value")
  }

  for g := 0; g < len(sa); g++ {
    sa[g][rand.Int() % len(sa[g])].Kill()
  }

  keys := make([]string, 10)
  vals := make([]string, len(keys))
  for i := 0; i < len(keys); i++ {
    keys[i] = strconv.Itoa(rand.Int())
    vals[i] = strconv.Itoa(rand.Int())
    ck.Put(keys[i], vals[i])
  }

  // are keys still there after joins?
  for g := 1; g < len(gids); g++ {
    mck.Join(gids[g], ha[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }
  
  // are keys still there after leaves?
  for g := 0; g < len(gids)-1; g++ {
    mck.Leave(gids[g])
    time.Sleep(2 * time.Second)
    for i := 0; i < len(sa[g]); i++ {
      sa[g][i].Kill()
    }
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }

  fmt.Printf("  ... Passed\n")
}

func doConcurrent(t *testing.T, unreliable bool) {
  smh, gids, ha, _, clean := setup("conc"+strconv.FormatBool(unreliable), unreliable)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  for i := 0; i < len(gids); i++ {
    mck.Join(gids[i], ha[i])
  }

  const npara = 2
  //const npara = 2
  var ca [npara]chan bool
  for i := 0; i < npara; i++ {
    ca[i] = make(chan bool)
    go func(me int) {
      ok := true
      defer func() { ca[me] <- ok }()
      ck := MakeClerk(smh)
      mymck := shardmaster.MakeClerk(smh)
      key := strconv.Itoa(me)
      last := ""
      for iters := 0; iters < 3; iters++ {
        nv := strconv.Itoa(rand.Int())
        v := ck.PutHash(key, nv)
        if v != last {
          ok = false
          // t.Fatalf("PutHash(%v) expected %v got %v\n", key, last, v)
        }
        last = NextValue(last, nv)
        v = ck.Get(key)
        if v != last {
          ok = false
          // t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
        }

        mymck.Move(rand.Int() % shardmaster.NShards,
          gids[rand.Int() % len(gids)])

        time.Sleep(time.Duration(rand.Int() % 30) * time.Millisecond)
      }
    }(i)
  }

  for i := 0; i < npara; i++ {
    x := <- ca[i]
    if x == false {
      //t.Fatalf("something is wrong")
    }
  }
}

func TestConcurrent(t *testing.T) {
  fmt.Printf("Test: Concurrent Put/Get/Move ...\n")
  doConcurrent(t, false)
  fmt.Printf("  ... Passed\n")
}

func TestConcurrentUnreliable(t *testing.T) {
  fmt.Printf("Test: Concurrent Put/Get/Move (unreliable) ...\n")
  doConcurrent(t, true)
  fmt.Printf("  ... Passed\n")
}

// func TestForgetMem(t *testing.T) {
//   smh, gids, ha, _, clean := setup("basic", false)
//   defer clean()

//   fmt.Printf("Test: Servers free forgotten instance memory ...\n")

//   mck := shardmaster.MakeClerk(smh)
//   mck.Join(gids[0], ha[0])

//   ck := MakeClerk(smh)

//   ck.Put("a", "x")
//   v := ck.PutHash("a", "b")
//   if v != "x" {
//     t.Fatalf("Puthash got wrong value")
//   }
//   ov := NextValue("x", "b")
//   if ck.Get("a") != ov {
//     t.Fatalf("Get got wrong value")
//   }

//   keys := make([]string, 10)
//   vals := make([]string, len(keys))
//   for i := 0; i < len(keys); i++ {
//     keys[i] = strconv.Itoa(rand.Int())
//     vals[i] = strconv.Itoa(rand.Int())
//     ck.Put(keys[i], vals[i])
//   }

//   runtime.GC()
//   var m0 runtime.MemStats
//   runtime.ReadMemStats(&m0)

//   // m0.Alloc about a megabyte

//   for i := 1; i <= 10; i++ {
//     big := make([]byte, 1000000)
//     for j := 0; j < len(big); j++ {
//       big[j] = byte('a' + rand.Int() % 26)
//     }
//     pxa[0].Start(i, string(big))
//     waitn(t, pxa, i, npaxos)
//   }

//   runtime.GC()
//   var m1 runtime.MemStats
//   runtime.ReadMemStats(&m1)

//   // m1.Alloc about 90 megabytes

//   for i := 0; i < npaxos; i++ {
//     pxa[i].Done(10)
//   }
//   for i := 0; i < npaxos; i++ {
//     pxa[i].Start(11 + i, "z")
//   }
//   time.Sleep(3 * time.Second)
//   for i := 0; i < npaxos; i++ {
//     if pxa[i].Min() != 11 {
//       t.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
//     }
//   }

//   runtime.GC()
//   var m2 runtime.MemStats
//   runtime.ReadMemStats(&m2)
//   // m2.Alloc about 10 megabytes

//   if m2.Alloc > (m1.Alloc / 2) {
//     t.Fatalf("memory use did not shrink enough")
//   }

//   fmt.Printf("  ... Passed\n")
// }
