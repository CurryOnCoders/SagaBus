/// <reference path='typings/angularjs/angular.d.ts' />
// TODO: Add better error handling
var Blog;
(function (Blog) {
    var blog = angular.module("blog", []);
    blog.service("blogUserService", ["$http", function ($http) {
            var currentUser = { identity: "$anonymous_user", isLoggedIn: false, name: "Anonymous User", email: null };
            return {
                currentUser: currentUser,
                logIn: function (userName, password) {
                    // TODO: Call server to validate credentials 
                },
                register: function (userName, password, email) {
                    // TODO: Call server to create new user
                }
            };
        }]);
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
    blog.controller("blogNavigation", ["$scope", "$http", "blogUserService",
        function ($scope, $http, userService) {
            $scope.currentPage = "home";
            $scope.service = userService;
            $scope.navigateTo = function (page) { return $scope.currentPage = page; };
        }]);
    blog.controller("blogHome", ["$scope", "$http", "blogPostService",
        function ($scope, $http, postService) {
            $scope.service = postService;
            $http.get("api/blog").then(function (payload) {
                $scope.blog = payload.data;
            }, function (error) { return alert(error); });
        }]);
})(Blog || (Blog = {}));
//# sourceMappingURL=index.js.map