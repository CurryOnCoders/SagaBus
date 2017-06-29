namespace CurryOn.Blog.Domain

open System

[<CLIMutable>]
type BlogPostComment = 
    { User: string; 
      Comment: string;
    }

[<CLIMutable>]
type BlogPost = 
    { Id: int64; 
      Title: string; 
      Author: string; 
      Date: DateTime; 
      Body: string; 
      Comments: BlogPostComment list; 
    }

[<CLIMutable>]
type BlogPostInfo =
    { Title: string; 
      Author: string; 
      Summary: string; 
      Date: DateTime; 
      BlogPostId: int64; 
    }

[<CLIMutable>]
type Blog = 
    { Name: string; 
      Description: string; 
      Posts: BlogPostInfo list; 
    }