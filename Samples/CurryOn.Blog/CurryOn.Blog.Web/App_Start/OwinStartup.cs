using Owin;
using Microsoft.Owin.Cors;
using Microsoft.Owin.StaticFiles;

namespace CurryOn.Blog.Web
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            app.UseCors(CorsOptions.AllowAll)
               .UseStaticFiles(new StaticFileOptions())
               .MapSignalR();
        }
    }
}