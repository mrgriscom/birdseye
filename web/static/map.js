

$(document).ready(function() {
	var cached = new L.TileLayer('/tile?z={z}&x={x}&y={y}');

	var map = new L.Map('map', {layers: [cached]});
	var london = new L.LatLng(51.505, -0.09);
	map.setView(new L.LatLng(30., 0.), 2);
    });