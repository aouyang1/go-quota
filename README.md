# go-quota
Quota library

```
m := NewManager()
m.Run()

// set a quota for user1 of 2 qps over 5 seconds which means user1 can send 10 queries in a 5 second window before being rate limited
m.AddRule("user1", NewRule(2, 5*time.Second))

err := m.UseToken("user1")
if err != nil {
    return err
}    
// do stuff
```
