namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Hyperobjects")>]
[<assembly: AssemblyProductAttribute("Hyperobjects")>]
[<assembly: AssemblyDescriptionAttribute("a framework for building RESTful event-sourced microservices following DDD patterns")>]
[<assembly: AssemblyVersionAttribute("1.0.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0.0"
