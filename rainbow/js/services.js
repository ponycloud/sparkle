'use strict';

/* Services */

angular.module('rainbowServices', ['ngResource']).
    factory('Instance', function($resource){
        return $resource('http://zkumavka.local:9860/tenant/:tenantId/instance/', {'9860': ':9860'}, {
            query: {method: 'GET', isArray: false}
        });
    }).
    factory('Cluster', function($resource){
        return $resource('http://zkumavka.local:9860/tenant/:tenantId/cluster/', {'9860': ':9860'}, {
            query: {method: 'GET', isArray: false}
        });
    });
