namespace CurryOn.Blog.Api

open CurryOn.Blog.Domain
open System
open System.Web.Http

type BlogController() = 
    inherit ApiController()
    member __.Get () =
        { Name = "Test Blog"; Description = "This is a test until some real data is available"; 
          Posts = [{ Title = "Fake First Post"; 
                     Author = "Aaron Eshbach";
                     Summary = "A placeholder post until real blog posts are available to display in the UI.";
                     Date = DateTime.Now;
                     BlogPostId = 1L; }];
        }