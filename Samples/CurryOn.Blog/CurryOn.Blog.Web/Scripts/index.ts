/// <reference path='typings/angularjs/angular.d.ts' />

module Blog {

    export interface IBlogPostComment { user: string; comment: string }
    export interface IBlogPost { id: number; title: string; author: string; date: Date; body: string; comments: IBlogPostComment[]; }
    export interface IBlogPostInfo { title: string; author: string; summary: string; date: Date; blogPostId: number; }
    export interface IBlog { name: string; description: string; posts: IBlogPostInfo[]; }
    export interface ISelectedBlogPost { postId: number; post: IBlogPost;}
    export interface IBlogPostService { currentPostId: number; currentPost: IBlogPost; loadBlogPost: Function; toIndex: Function; }
    export interface IBlogHomeScope extends ng.IScope { blog: IBlog; service: IBlogPostService; }

    var blog = angular.module("blog", []);

    blog.service("blogPostService", ["$http", ($http: ng.IHttpService) => {
        var selectedPost = <ISelectedBlogPost>{ postId: -1, post: null };

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

    // TODO: Add better error handling
    blog.controller("blogHome", ["$scope", "$http", "blogPostService",
        ($scope: IBlogHomeScope, $http: ng.IHttpService, blogPostService: IBlogPostService) => {
            $scope.service = blogPostService;
            $http.get("api/blog").then((payload: ng.IHttpPromiseCallbackArg<IBlog>) => {
                $scope.blog = payload.data;
            }, (error) => alert(error)); 
        }]);
}