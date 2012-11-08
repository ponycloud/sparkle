'use strict';

/* Controllers */

function InstanceListCtrl($scope, $routeParams, Instance) {
    $scope.instances = Instance.get({'tenantId': $routeParams.tenantId});

}

function ClusterListCtrl($scope, $routeParams, Cluster) {
    $scope.clusters = Cluster.get({'tenantId': $routeParams.tenantId});

}



/*
function PhoneDetailCtrl($scope, $routeParams, Phone) {
    $scope.phone = Phone.get({phoneId: $routeParams.phoneId}, function(phone) {
        $scope.mainImageUrl = phone.images[0];
    });

    $scope.setImage = function(imageUrl) {
        $scope.mainImageUrl = imageUrl;
    }
}
*/
