'use strict';

/* App Module */



var app = angular.module('rainbow', ['rainbowServices']).
    config(['$routeProvider', function($routeProvider) {
    $routeProvider.
        when('/:tenantId/instance', {templateUrl: 'partials/instance-list.html',   controller: InstanceListCtrl}).
        when('/:tenantId/cluster', {templateUrl: 'partials/cluster-list.html',   controller: ClusterListCtrl}).
        otherwise({redirectTo: '/instances'});
}]);


app.run(function($rootScope, $routeParams, $location) {
    $rootScope.getClass = function(name) {
        var re = new RegExp("/"+name,"i");
        console.log(name + " " + re.test($location.path()));

        if (re.test($location.path())) {
            return "active";
        }
    }
    $rootScope.params = $routeParams;    
});
