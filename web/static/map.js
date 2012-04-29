

$(document).ready(function() {
	//var google = new L.TileLayer('http://mt{s}.gwoogle.com/vt/x={x}&y={y}&z={z}', {subdomains: '0123'});
	//var google = new L.TileLayer('http://tileserver.mytopo.com/SecureTile/TileHandler.ashx?mapType=Topo&partnerID=12288&hash=0FCF7E00AE7F7AAF6B7A18CF387B17ED&x={x}&y={y}&z={z}');
	//var google = new L.TileLayer('http://{s}.tile.openstreetmap.us/tiger2011_roads/{z}/{x}/{y}.png');

	var map = new L.Map('map');
	map.setView(new L.LatLng(30., 0.), 2);

	$.get('/layers', null, function(data) {
		var layers = {};
		$.each(data, function(i, e) {
			var layer = new L.TileLayer('/tile/' + e.id + '/{z}/{x},{y}');
			layers[e.name] = layer;
			map.addLayer(layer);
		    });

		var layersControl = new L.Control.Layers(layers, {});
		map.addControl(layersControl);

		var canvas_layer = new L.TileLayer.Canvas();
		canvas_layer.drawTile = function(canvas, tile, zoom) {
		    $.get('/tilecover/googmap/' + zoom + '/' + tile.x + ',' + tile.y, function(data) {
			    var ctx = canvas.getContext('2d');
			    ctx.fillStyle = 'rgba(255, 0, 0, 0.1)';

			    $.each(data, function(i, t) {
				    var w = 256 * Math.pow(0.5, t.z);
				    ctx.fillRect(w * t.x, w * t.y, w, w);  
				});
			}, 'json');
		}
		map.addLayer(canvas_layer);

	    }, 'json');

    });