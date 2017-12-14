	
var app = angular.module('myApp',[]);

app.controller('LocationsController',['$scope', '$http', function($scope, $http){
	
	
	$scope.limit = 10;
	$scope.cfArray = ["2017Q2", "2017Q1", "2016Q4", "2016Q3", "2016Q2", "2016Q1"];

	$scope.getTopLoc= function (cf) {
    	$http({
            method: 'GET',
            url:'/assets/' + cf + '/topkLocations/part-r-00000',
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topLocations = data;
        });
    	
    	
    	$http({
            method: 'GET',
            url:'/assets/' + cf + '/topkOrigins/part-r-00000',
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topOrigins = data; 
        });
    	
    	$http({
            method: 'GET',
            url:'/assets/' + cf + '/topkDestinations/part-r-00000',
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topDestinations = data; 
        });
    	
    	
    	
	}
    $scope.getLocAgg = function(){
    	
    	$http({
            method: 'GET',
            url:'hbase/' + $scope.src + '/' + $scope.des,
            params:{
            	"src": $scope.src,
            	"des": $scope.des
            }   
        }).success(function(data){
            $scope.location = data;
            console.log(data)
    		            
        });
    } 
    
    
}]);

app.controller('CarriersController',['$scope', '$http', function($scope, $http){
	$scope.limit = 10;
	$scope.cfArray = ["2017Q2", "2017Q1", "2016Q4", "2016Q3", "2016Q2", "2016Q1"];
		
	$scope.getTopCar = function (cf) {
    	$http({
            method: 'GET',
            url:'/assets/' + cf + '/topkOpCarrier/part-r-00000',
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topOpCar = data; 
        });
    	
    	
    	$http({
            method: 'GET',
            url:'/assets/' + cf + '/topkTktCarrier/part-r-00000',
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topTktCar = data; 
        });
    	
    	$http({
            method: 'GET',
            url:'/assets/' + cf + '/TopRevTktCarrier/part-r-00000',
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topTktRevCar = data; 
        });
    	
	}
	$scope.getCarAgg = function(){
    	
    	$http({
            method: 'GET',
            url:'api/v1/getCarrierAgg',
            params:{
            	"cc": $scope.cc
            }   
        }).success(function(data){
        	console.log(data);
            $scope.carrierArr = data;
        });
    }       
	
}]);

app.controller('RoundtripController',['$scope', '$http', function($scope, $http){
	$http({
        method: 'GET',
        url:'/assets/rt.json',
        dataType: 'json',
        headers: {
        	'Access-Control-Allow-Origin': '*'
        }
    }).success(function(data){
    	console.log(data)
        $scope.rt = data; 

    });
}]);
	    	

