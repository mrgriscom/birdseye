

function draw_pinpoint(highlight, halo, ctx, w, h) {
    var circle = function(rad) {
	ctx.beginPath();
	ctx.arc(.5*w, .5*h, rad, 0, 2*Math.PI);
	ctx.closePath();
    }

    circle(5);
    ctx.fillStyle = (highlight ? '#ff8' : '#06f');
    ctx.fill();

    circle(5);
    ctx.strokeStyle = '#222';
    ctx.lineWidth = 1;
    ctx.stroke();

    if (halo) {
	circle(9);
	ctx.strokeStyle = 'rgba(255, 0, 0, .5)';
	ctx.lineWidth = 2;
	ctx.stroke();
    }
}

function Region(map) {
    var ICON_DEFAULT = render_marker(function(ctx, w, h) { draw_pinpoint(false, false, ctx, w, h); }, 20, 20);
    var ICON_ACTIVE = render_marker(function(ctx, w, h) { draw_pinpoint(true, true, ctx, w, h); }, 20, 20);
    var ICON_NEXT = render_marker(function(ctx, w, h) { draw_pinpoint(false, true, ctx, w, h); }, 20, 20);

    this.points = [];
    this.vertexes = [];
    this.poly = new L.Polygon(this.vertexes);
    this.active = null;

    map.addLayer(this.poly);

    this.new_point = function(e) {
	var marker = new L.Marker(e.latlng, {
		draggable: true,
		icon: ICON_DEFAULT
	    });

	var r = this;
	marker.on('drag', function(e) {
		r.vertexes.splice(r.find_point(marker), 1, marker.getLatLng());
		r.update();
	    });
	marker.on('click', function(e) {
		r.set_active(marker);
	    });

	map.addLayer(marker);
	this.insert_point(marker);
	this.update();

	this.set_active(marker);
    }

    this.set_active = function(p) {
	if (this.active) {
	    this.active.setIcon(ICON_DEFAULT);
	    var next = this.next_point(this.active);
	    if (next) {
		next.setIcon(ICON_DEFAULT);
	    }
	}

	this.active = p;
	if (this.active == null) {
	    return;
	}

	this.active.setIcon(ICON_ACTIVE);
	var next = this.next_point(this.active);
	if (next) {
	    next.setIcon(ICON_NEXT);
	}
    }

    this.insert_point = function(p) {
	var i = this.find_point(this.active);
	this.points.splice(i + 1, 0, p);
	this.vertexes.splice(i + 1, 0, p.getLatLng());
    }

    this.delete_point = function(p) {
	if (p == null) {
	    return;
	}

	var active = this.active;
	if (p == active) {
	    active = this.prev_point(p);
	}

	var i = this.find_point(p);
	this.points.splice(i, 1);
	this.vertexes.splice(i, 1);

	this.set_active(active);
	this.update();
	map.removeLayer(p);
    }

    this.find_point = function(p) {
	return this.points.indexOf(p);
    }

    this.next_point = function(p) {
	if (this.points.length > 1) {
	    return this.points[(this.find_point(p) + 1) % this.points.length];
	} else {
	    return null;
	}
    }

    this.prev_point = function(p) {
	if (this.points.length > 1) {
	    return this.points[(this.find_point(p) + this.points.length - 1) % this.points.length];
	} else {
	    return null;
	}
    }

    this.update = function() {
	this.poly.setLatLngs(this.vertexes);
    }
}

$(document).ready(function() {
	var map = new L.Map('map');
	map.setView(new L.LatLng(30., 0.), 2);

	var r = new Region(map);
	map.on('click', function(e) { r.new_point(e); });

	shortcut.add('backspace', function() {
		r.delete_point(r.active);
	    });

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
		//map.addLayer(canvas_layer);

		/*
var nexrad = new L.TileLayer.WMS("http://mesonet.agron.iastate.edu/cgi-bin/wms/nexrad/n0r.cgi", {
    layers: 'nexrad-n0r-900913',
    format: 'image/png',
    transparent: true,
    attribution: "Weather data © 2012 IEM Nexrad"
});
map.addLayer(nexrad);
		*/



	    }, 'json');

    });


/* todo

ignore new marker event if already a marker that exact spot


 */




function make_canvas(w, h) {
    var $canvas = $('<canvas />');
    $canvas.attr('width', w);
    $canvas.attr('height', h);
    return $canvas;
}

function canvas_context(canvas) {
    var ctx = canvas.getContext('2d');
    ctx.clear = function() {
	ctx.save();
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.clearRect(0, 0, canvas.width, canvas.height);
	ctx.restore();
    };
    return ctx;
}

// draw to a canvas and export the result as an image (data url)
function render_icon(draw, width, height) {
    var canvas = make_canvas(width, height)[0];
    var ctx = canvas_context(canvas);
    draw(ctx, width, height);
    return canvas.toDataURL('image/png');
}

// create an icon rendered via canvas
function render_marker(draw, w, h, anchor) {
    anchor = anchor || [0, 0];
    var icon = L.Icon.extend({
	    iconUrl: render_icon(draw, w, h),
	    shadowUrl: null,
	    iconSize: new L.Point(w, h),
	    iconAnchor: new L.Point(w * .5 * (anchor[0] + 1.), h * .5 * (1. - anchor[1])),
	});
    return new icon();
}

