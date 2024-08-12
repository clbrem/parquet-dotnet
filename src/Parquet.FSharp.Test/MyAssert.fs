namespace MyAssert

type Assert<'T> () =
    inherit Xunit.Assert() with
      static member FailWith f (a: 'T)=
           Assert.Fail(sprintf f a) 
      static member EqualTo (a:'T) (b:'T) =
          Assert.Equal<'T>(a,b)
      static member Pass =
          ()  

