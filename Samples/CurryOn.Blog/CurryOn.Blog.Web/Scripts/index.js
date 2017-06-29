/// <reference path='typings/angularjs/angular.d.ts' />
var Blog;
(function (Blog) {
    var blog = angular.module("blog", []);
    blog.service("blogPostService", ["$http", function ($http) {
            var selectedPost = { postId: -1, post: null };
            return {
                currentPostId: selectedPost.postId,
                currentPost: selectedPost.post,
                loadBlogPost: function (id) {
                    $http.get("api/blog/" + id).then(function (payload) {
                        selectedPost.post = payload.data;
                        selectedPost.postId = payload.data.id;
                    }, function (error) { return alert(error); });
                },
                toIndex: function () { return selectedPost.postId = -1; }
            };
        }]);
    // TODO: Add better error handling
    blog.controller("blogHome", ["$scope", "$http", "blogPostService",
        function ($scope, $http, blogPostService) {
            $scope.service = blogPostService;
            $http.get("api/blog").then(function (payload) {
                $scope.blog = payload.data;
            }, function (error) { return alert(error); });
        }]);
})(Blog || (Blog = {}));
//# sourceMappingURL=index.js.map