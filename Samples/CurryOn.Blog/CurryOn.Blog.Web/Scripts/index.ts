/// <reference path='typings/angularjs/angular.d.ts' />

// TODO: Add better error handling
module Blog {
    export interface IBlogPostComment { user: string; comment: string }
    export interface IBlogPost { id: number; title: string; author: string; date: Date; body: string; comments: IBlogPostComment[]; }
    export interface IBlogPostInfo { title: string; author: string; summary: string; date: Date; blogPostId: number; }
    export interface IBlog { name: string; description: string; posts: IBlogPostInfo[]; }
    export interface ISelectedPost { postId: number; post: IBlogPost; }
    export interface IUser { identity: string; isLoggedIn: boolean; name: string; email: string; }
    export interface IBlogUserService { currentUser: IUser; logIn: Function; register: Function; }
    export interface IBlogPostService { currentPostId: number; currentPost: IBlogPost; loadBlogPost: Function; toIndex: Function; }
    export interface IBlogNavigationScope extends ng.IScope { currentPage: string; navigateTo: Function; service: IBlogUserService; }
    export interface IBlogHomeScope extends ng.IScope { blog: IBlog; service: IBlogPostService; }

    var blog = angular.module("blog", []);

    blog.service("blogUserService", ["$http", ($http: ng.IHttpService) => {
        var currentUser = <IUser>{ identity: "$anonymous_user", isLoggedIn: false, name: "Anonymous User", email: null };
        return <IBlogUserService>{
            currentUser: currentUser,
            logIn: (userName: string, password: string) => { 
                // TODO: Call server to validate credentials 
            },
            register: (userName: string, password: string, email: string) => {
                // TODO: Call server to create new user
            }
        };
    }]);

    blog.service("blogPostService", ["$http", ($http: ng.IHttpService) => {
        var selectedPost = <ISelectedPost>{ postId: -1, post: null };

        return <IBlogPostService>{
            currentPostId: selectedPost.postId,
            currentPost: selectedPost.post,
            loadBlogPost: (id: number) => {
                $http.get("api/blog/" + id).then((payload: ng.IHttpPromiseCallbackArg<IBlogPost>) => {
                    selectedPost.post = payload.data;
                    selectedPost.postId = payload.data.id;
                }, (error) => alert(error));
            },
            toIndex: () => selectedPost.postId = -1
        };
    }]);

    blog.controller("blogNavigation", ["$scope", "$http", "blogUserService",
        ($scope: IBlogNavigationScope, $http: ng.IHttpService, userService: IBlogUserService) => {
            $scope.currentPage = "home";
            $scope.service = userService;
            $scope.navigateTo = (page: string) => $scope.currentPage = page;
        }]);
    
    blog.controller("blogHome", ["$scope", "$http", "blogPostService",
        ($scope: IBlogHomeScope, $http: ng.IHttpService, postService: IBlogPostService) => {
            $scope.service = postService;
            $http.get("api/blog").then((payload: ng.IHttpPromiseCallbackArg<IBlog>) => {
                $scope.blog = payload.data;
            }, (error) => alert(error)); 
        }]);
}