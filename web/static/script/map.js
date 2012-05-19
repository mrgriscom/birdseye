

function draw_pinpoint(highlight, halo, ctx, w, h) {
    var circle = function(rad) {
	ctx.beginPath();
	ctx.arc(.5*w, .5*h, rad, 0, 2*Math.PI, false);
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

function RegionPoly(map, points) {
    var ICON_DEFAULT = render_marker(function(ctx, w, h) { draw_pinpoint(false, false, ctx, w, h); }, 20, 20);
    var ICON_ACTIVE = render_marker(function(ctx, w, h) { draw_pinpoint(true, true, ctx, w, h); }, 20, 20);
    var ICON_NEXT = render_marker(function(ctx, w, h) { draw_pinpoint(false, true, ctx, w, h); }, 20, 20);

    this.points = [];
    this.vertexes = [];
    this.poly = new L.Polygon(this.vertexes);
    this.active = null;

    this.init = function() {
	this.import_points(points || []);
	map.addLayer(this.poly);
    }

    this.new_point = function(e) {
	var marker = new L.Marker(this.rectify_lon(e.latlng), {
		draggable: true,
		icon: ICON_DEFAULT
	    });
	var r = this;
	marker.on('drag', function(e) {
		var pos = r.rectify_lon(marker.getLatLng(), r.adjacent_point(marker, false));
		// update marker with corrected position
		marker.setLatLng(pos);
		r.vertexes.splice(r.find_point(marker), 1, pos);
		r.update();
	    });
	marker.on('click', function(e) {
		r.set_active(marker);
	    });

	map.addLayer(marker);
	this.insert_point(marker);
	this.set_active(marker);
	this.update();
    }

    this.rectify_lon = function(ll, ref) {
	// correct lon so it's within 180 degrees of lon of ref point;
	// this lets us handle polygons that straddle the IDL
	ref = ref || this.active;
	if (ref != null) {
	    var rect_lon = anglenorm(ll.lng, 180. - ref.getLatLng().lng);
	    return new L.LatLng(ll.lat, rect_lon, true);
	} else {
	    return ll;
	}
    }

    this.delete_point = function(p) {
	var new_active = (p == this.active ? this.adjacent_point(p, false) : this.active);

	map.removeLayer(p);
	this.remove_point(p);
	this.set_active(new_active);
	this.update();
    }

    this.set_active = function(p) {
	var r = this;
	var set_icons = function(on) {
	    if (r.active) {
		r.active.setIcon(on ? ICON_ACTIVE : ICON_DEFAULT);
		var next = r.adjacent_point(r.active, true);
		if (next) {
		    next.setIcon(on ? ICON_NEXT : ICON_DEFAULT);
		}
	    }
	}

	set_icons(false);
	this.active = p;
	set_icons(true);
    }

    this.insert_point = function(p) {
	var i = this.find_point(this.active);
	this.points.splice(i + 1, 0, p);
	this.vertexes.splice(i + 1, 0, p.getLatLng());
    }

    this.remove_point = function(p) {
	var i = this.find_point(p);
	this.points.splice(i, 1);
	this.vertexes.splice(i, 1);
    }

    this.delete_active = function() {
	if (this.active) {
	    this.delete_point(this.active);
	}
    }

    this.find_point = function(p) {
	return this.points.indexOf(p);
    }

    this.adjacent_point = function(p, next) {
	if (this.points.length > 1) {
	    var offset = (next ? 1 : -1);
	    return this.points[(this.find_point(p) + this.points.length + offset) % this.points.length];
	} else {
	    return null;
	}
    }

    this.update = function() {
	this.poly.setLatLngs(this.vertexes);
    }

    this.import_points = function(points) {
	var r = this;
	$.each(points, function(i, p) {
		r.new_point({latlng: new L.LatLng(p[0], p[1])});
	    });
    }

    this.bounds = function() {
	var coords = [];
	$.each(this.vertexes, function(i, ll) {
		coords.push([ll.lat, anglenorm(ll.lng)]);
	    });
	return coords;
    }

    this.bounds_str = function(prec) {
	prec = prec || 5;
	var cfmt = [];
	$.each(this.bounds(), function(i, c) {
		cfmt.push(c[0].toFixed(prec) + ',' + c[1].toFixed(prec));
	    });
	return cfmt.join(' ');
    }

    this.is_degenerate = function() {
	return this.vertexes.length < 3;
    }

    this.init();
}

function tile_url(spec, zoom, point) {
    var replace = function(key, sub) {
	spec = spec.replace(new RegExp('{' + key + '(:[^}]+)?}', 'g'), function(match, submatch) {
		return sub(submatch == null || submatch.length == 0 ? null : submatch.substring(1));
	    });
    }

    replace('z', function() { return zoom; });
    replace('x', function() { return point.x; });
    replace('y', function() { return point.y; });
    replace('-y', function() { return Math.pow(2, zoom) - 1 - point.y; });
    replace('s', function(arg) {
	    var k = point.x + point.y;
	    if (arg.indexOf('-') == -1) {
		return arg.split('')[k % arg.length];
	    } else {
		var bounds = arg.split('-');
		var min = +bounds[0];
		var max = +bounds[1];
		return min + k % (max - min + 1);
	    }
	});
    replace('qt', function(arg) {
	    var bin_digit = function(h, i) {
		return Math.floor(h / Math.pow(2, i) % 2);
	    }

	    var qt = '';
	    for (var i = zoom - 1; i >= 0; i--) {
		var q = 2 * bin_digit(point.y, i) + bin_digit(point.x, i);
		qt += (arg != null ? arg[q] : q);
	    }
	    return qt;
	});
    replace('custom', function(arg) {
	    // note: this blocks the browser due to need for synchronous request to server
	    var url = null;
	    $.ajax('/tileurl/' + arg + '/' + zoom + '/' + point.x + ',' + point.y, {
		    success: function(data) {
			url = data;
		    },
		    async: false
		});
	    return url;
	});

    return spec;
}

$(document).ready(function() {

	// monkey patch to add 'alpha-channel checker' bg to all tiles
	var tile_bg = render_icon(alpha_checker, 256, 256);
	var _onload = L.TileLayer.prototype._tileOnLoad;
	L.TileLayer.prototype._tileOnLoad = function(e) {
	    if (this.src && this.src.substring(0, 5) != 'data:') {
		$(this).css('background', 'url(' + tile_bg + ')');
	    }
	    _onload.call(this, e);
	};

	var DEFAULT_ZOOM = 2;

	var map = new L.Map('map', {worldCopyJump: false});
	map.setView(new L.LatLng(30., 0.), DEFAULT_ZOOM);

	var r = new RegionPoly(map);
	map.on('click', function(e) { r.new_point(e); });
	shortcut.add('backspace', function() {
		r.delete_active();
	    });

	var _p = new ActiveLoc(map);
	map.on('mousemove', function(e) {
		_p.update(e);
		_p.refresh_info();
	    });
	map.on('zoomend', function() {
		_p.refresh_info();
	    });
	map.on('mouseout', function() {
		_p.update(null);
		_p.refresh_info();
	    });
	map.on('move', function() {
		_p.refresh_info();
	    });
	_p.refresh_info();

	//debug
	shortcut.add('q', function() {
		console.log(r.bounds_str());
	    });

	$.get('/layers', {default_zoom: DEFAULT_ZOOM}, function(data) {
		var defaultLayer = null;
		var layersControl = new LayerControl();
		$.each(data, function(i, e) {
			layersControl.addBaseLayer(e, e.name);
			if (e.default) {
			    defaultLayer = e;
			}
		    });
		map.addControl(layersControl);
		layersControl.select(defaultLayer);
		layersControl.select('source');
		layersControl.select('cached');
	    }, 'json');

    });

function ActiveLoc(map) {
    this.p = null;

    this.update = function(mouse_evt) {
	this.p = (mouse_evt != null ? map.layerPointToContainerPoint(mouse_evt.layerPoint) : null);
    }

    this.refresh_info = function() {
	var ll = (this.p ? map.layerPointToLatLng(map.containerPointToLayerPoint(this.p)) : map.getCenter());
	update_info(ll, map);
    }
}

function pos_info(ll, map) {
    var tile_coord = function(k) {
	return Math.floor(k / 256.);
    };

    var info = {
	lat: ll.lat,
	lon: ll.lng,
	zoom: map.getZoom(),
	effzoom_offset: Math.floor(Math.log(Math.cos(Math.PI * ll.lat / 180.)) / Math.log(0.5))
    };

    if (Math.abs(ll.lat) <= L.Projection.SphericalMercator.MAX_LATITUDE) {
	var px = map.project(ll);
	info.tx = tile_coord(px.x);
	info.ty = tile_coord(px.y);
	info.qt = tile_url('{qt}', info.zoom, new L.Point(info.tx, info.ty));
    }

    return info;
}

function update_info(ll, map) {
    var $info = $('#info');
    var info = pos_info(ll, map);

    var npad = function(n, pad) {
	var s = '' + n;
	while (s.length < pad) {
	    s = '0' + s;
	}
	return s;
    }

    var fmt_ll = function(k, dir, pad) {
	var PREC = 5;
	return dir[k >= 0 ? 0 : 1] + npad(Math.abs(k).toFixed(PREC), PREC + 1 + pad) + '\xb0';
    };
    
    var max_t = Math.pow(2, info.zoom) - 1;
    var fmt_t = function(t, z) {
	return npad(t, ('' + max_t).length);
    }

    $info.find('#lat').text(fmt_ll(info.lat, 'NS', 2));
    $info.find('#lon').text(fmt_ll(info.lon, 'EW', 3));
    $info.find('#zoom').text(info.zoom);
    $info.find('#effzoom').text(info.zoom + info.effzoom_offset);
    $info.find('#zeff')[info.effzoom_offset == 0 ? 'hide' : 'show']();    
    $info.find('#tx').text(info.tx != null ? fmt_t(info.tx) : '\u2013');
    $info.find('#ty').text(info.ty != null ? fmt_t(info.ty) : '\u2013');
    $info.find('#qt').text(info.qt == null ? '\u2013' : (info.qt || '\u2205'));
}

var no_cache = render_icon(non_cached, 256, 256);
function cache_layer(lyrspec, notfound) {
    return new L.TileLayer('/tile/' + lyrspec.id + '/{z}/{x},{y}', {
	    errorTileUrl: notfound ? no_cache : null
	});
}

//url_param('mode', 'proxy')
function source_layer(lyrspec, proxy, notfound) {
    if (proxy) {
	return new L.TileLayer('/tileproxy/' + lyrspec.id + '/{z}/{x},{y}');
    } else {
	var reflayer = new L.TileLayer();
	reflayer.getTileUrl = function(tilePoint, zoom) {
	    // warning: referer may be leaked to map server!
	    return tile_url(lyrspec.url, zoom, tilePoint);
	};
	return reflayer;
    }
}

function coverage_layer(lyrspec) {
    var canvas_layer = new L.TileLayer.Canvas();
    canvas_layer.drawTile = function(canvas, tile, zoom) {
	$.get('/tilecover/' + lyrspec.id + '/' + zoom + '/' + tile.x + ',' + tile.y, function(data) {
		var ctx = canvas.getContext('2d');
		ctx.fillStyle = 'rgba(255, 0, 0, 0.1)';
		
		$.each(data, function(i, t) {
			var w = 256 * Math.pow(0.5, t.z);
			ctx.fillRect(w * t.x, w * t.y, w, w);  
		    });
	    }, 'json');
    }
    return canvas_layer;
}

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

function alpha_checker(ctx, w, h) {
    var dim = 8;
    var colors = ['#787878', '#888888'];

    for (var i = 0, x = 0; x < w; x += dim, i++) {
	for (var j = 0, y = 0; y < h; y += dim, j++) {
	    ctx.fillStyle = colors[(i + j) % colors.length];
	    ctx.fillRect(x, y, dim, dim);
	}
    }
}

function non_cached(ctx, w, h) {
    ctx.globalAlpha = .2;
    ctx.fillStyle = 'green';
    ctx.fillRect(.2*w,.2*h,.6*w,.6*h);
}

function mod(a, b) {
    if (a < 0) {
	return ((a % b) + b) % b;
    } else {
	return a % b;
    }
}

function anglenorm(a, offset) {
    if (offset == null) {
	offset = 180.;
    }
    return mod(a + offset, 360.) - offset;
}

function url_param(param, value) {
    var url = window.location.href;
    var params = url.substring(url.indexOf('?') + 1);

    var _ = function(s) { return '&' + s + '&'; };
    return _(params).indexOf(_(param + '=' + value)) != -1;
}


LayerControl = L.Control.Layers.extend({
	setLabel: function(label) {
	    this.$label.text(label);
	    if (label != null && label.length > 0) {
		this.$label.removeClass('empty');
	    } else {
		this.$label.addClass('empty');
	    }
	},

	_initLayout: function() {
	    L.Control.Layers.prototype._initLayout.call(this);
	    
	    var $a = $(this._container).find('a');
	    $a.attr('id', 'layericon');
	    
	    this.$label = $('<div />');
	    this.$label.addClass('layerlabel');
	    this.$label.addClass('empty');
	    $a.append(this.$label);
	},

	initialize: function(layers) {
	    this.active_layers = {}
	    this.active_layer = null;

	    var overlay_types = ['source', 'cached', 'coverage'];
	    var ov = {};
	    var lc = this;
	    $.each(overlay_types, function(i, e) {
		    ov[e] = {id: e, overlay: true};
		    lc.active_layers[e] = null;
		});
	    L.Control.Layers.prototype.initialize.call(this, layers, ov);
	},

	_onInputClick: function() {
	    var inputs = $(this._form).find('input');
	    var lc = this;
	
	    var active_layer = null;
	    var active_overlays = [];
	    $.each(inputs, function(i, input) {
		    var o = lc._layers[input.layerId];
		    if (input.checked){
			if (o.overlay) {
			    active_overlays.push(o.layer.id);
			} else {
			    active_layer = o.layer;
			}
		    }
		});

	    $.each(this.active_layers, function(type, maplayer) {
		    // remove existing layers if overlay type deselected or layer type changed
		    if (maplayer && (active_layer != lc.active_layer || active_overlays.indexOf(type) == -1)) {
			lc._map.removeLayer(maplayer);
			lc.active_layers[type] = null;
			console.log('removing layer: ' + type);
		    }
		});
	    $.each(this.active_layers, function(type, maplayer) {
		    // add layers if layer type defined and overlay type selected and no layer already set
		    if (maplayer == null && active_layer != null && active_overlays.indexOf(type) != -1) {
			var maplayer = ({
				source: function(l) { return source_layer(l, url_param('mode', 'proxy'), true); },
				cached: function(l) { return cache_layer(l, true); },
				coverage: function(l) { return coverage_layer(l); },
			    }[type])(active_layer);
			lc._map.addLayer(maplayer);
			lc.active_layers[type] = maplayer;
			console.log('adding layer: ' + type + ' ' + active_layer.id);
		    }
		});
	    this.active_layer = active_layer;

	    if (this.active_layer) {
		var lab = this.active_layer.id;
		if (this.active_layers.source && !this.active_layers.cached) {
		    lab += ' (live)';
		}
	    } else {
		var lab = null;
	    }
	    this.setLabel(lab);
	},

	select: function(e) {
	    var inputs = $(this._form).find('input');
	    var lc = this;
	    $.each(inputs, function(i, input) {
		    var o = lc._layers[input.layerId];
		    if (o.layer == e || o.layer.id == e) {
			input.defaultChecked = true;
		    }
		});
	    this._onInputClick();
	}
    });
