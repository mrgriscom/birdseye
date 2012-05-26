
var DEFAULT_ZOOM = 2;
var MAX_ZOOM = 20;
var DL_COMMAND = 'python mapcache.py';
var ICON_PATH = '/img/leaflet';
var WAYPOINT_PREC = 4;

function init_console() {
    monkey_patch();

    L.Icon.Default.imagePath = ICON_PATH;
    var map = new L.Map('map', {
	    maxZoom: MAX_ZOOM,
	    worldCopyJump: false,
	});
    map.setView(new L.LatLng(30., 0.), DEFAULT_ZOOM);
    map.addControl(new L.Control.Scale({
            maxWidth: 125,
	    position: 'bottomright',                       
	}));
    var layersControl = new LayerControl();
    map.addControl(layersControl);

    var _p = new ActiveLoc(map);
    map.addControl(_p.mk_control());

    var _r = new RegionManager(map, function() {
	    return layersControl.active_layer;
        });

    map.addControl(new ManagementOptions({
		enable_regions: function() {
		    map.addControl(_r.mk_control());
		 
		    $.get('/regions', function(data) {
			    $.each(data, function(i, reg) {
				    _r.add_region(reg);
				});
			});
		},
		enable_waypoints: function() {
		    map.addControl(new NewWaypoint());
    
		    $.get('/waypoints', function(data) {
			    $.each(data, function(i, pt) {
				    var wpt = new Waypoint(new L.LatLng(pt.pos[0], pt.pos[1]), pt);
				    map.addLayer(wpt);
				});
			});
		},
	    }));

    $.get('/layers', {default_zoom: DEFAULT_ZOOM}, function(data) {
	    var defaultLayer = null;
	    $.each(data, function(i, e) {
		    layersControl.addBaseLayer(e, e.name);
		    if (e.default) {
			defaultLayer = e;
		    }
		    _r.add_layer(e);
		});
	    layersControl.select(defaultLayer);
	    layersControl.select('source');
	    //layersControl.select('cached');
	}, 'json');
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

var IMG_ALPHABG = '/img/alphabg.png';
var IMG_NOCACHE = '/img/nocache.png';

function monkey_patch() {
    // monkey patch to add 'alpha-channel checker' bg to all tiles
    var _onload = L.TileLayer.prototype._tileOnLoad;
    L.TileLayer.prototype._tileOnLoad = function(e) {
        if (this.src && this.src.indexOf(IMG_NOCACHE) == -1) {
            $(this).css('background', 'url(' + IMG_ALPHABG + ')');
        }
        _onload.call(this, e);
    };

    // 'lyrs' needs to contain all layers currently on map
    L.Map.prototype.order_layers = function(lyrs) {
        $(this._tilePane).empty();
        var m = this;
        $.each(lyrs, function(i, lyr) {
                $(m._tilePane).append(lyr._container);
            });
    }

    L.Handler.MarkerDrag.prototype._onDragStart = function(e) {
	if (!this._marker.options.dragPopup) {
	    this._marker.closePopup();
	}
	this._marker.fire('movestart').fire('dragstart');
    }

    var _ondrag = L.Handler.MarkerDrag.prototype._onDrag;
    L.Handler.MarkerDrag.prototype._onDrag = function(e) {
	_ondrag.call(this, e);
	if (this._marker._popup && this._marker.options.dragPopup) {
	    this._marker._popup.setLatLng(this._marker.getLatLng());
	}
    }
}

function ActiveLoc(map) {
    this.p = null;

    this.update = function(mouse_evt) {
        this.p = (mouse_evt != null ? map.layerPointToContainerPoint(mouse_evt.layerPoint) : null);
    }

    this.refresh_info = function() {
        var ll = (this.p ? map.layerPointToLatLng(map.containerPointToLayerPoint(this.p)) : map.getCenter());
        this.update_info(ll);
    }

    this.init = function() {
        var _p = this;
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
        this.refresh_info();
    }

    this.update_info = function(ll) {
	var $info = $('#info');
	var info = pos_info(ll, map);

	var max_t = Math.pow(2, info.zoom) - 1;
	var fmt_t = function(t, z) {
	    return npad(t, ('' + max_t).length);
	}

	var fp = fmt_pos(info.lat, info.lon, 5);
	$info.find('#lat').text(fp.lat);
	$info.find('#lon').text(fp.lon);
	$info.find('#zoom').text(info.zoom);
	$info.find('#effzoom').text(info.effzoom);
	$info.find('#zeff')[info.effzoom_offset == 0 ? 'hide' : 'show']();    
	$info.find('#tx').text(info.tx != null ? fmt_t(info.tx) : '\u2013');
	$info.find('#ty').text(info.ty != null ? fmt_t(info.ty) : '\u2013');
	$info.find('#qt').text(info.qt == null ? '\u2013' : (info.qt || '\u2205'));
    }

    this.mk_control = function() {
	var PosInfo = L.Control.extend({
		options: {
		    position: 'bottomleft'
		},

		onAdd: function(map) {
		    return ctrl_init('#info');
		},
	    });	
	this.init();
	return new PosInfo();
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
    info.effzoom = info.zoom + info.effzoom_offset;
	
    if (Math.abs(ll.lat) <= L.Projection.SphericalMercator.MAX_LATITUDE) {
	var px = map.project(ll);
	info.tx = tile_coord(px.x);
	info.ty = tile_coord(px.y);
	info.qt = tile_url('{qt}', info.zoom, new L.Point(info.tx, info.ty));
    }
    
    return info;
}

function RegionManager(map, get_active_layer) {
    this.all_regions = [];

    this.region = null;
    this.rpoly = null;

    this.editing = false;
    
    this.init = function() {
        var rm = this;
        $('#regions #new').click(function() {
                rm.activate();
            });
        $('#regions #submit').click(function() {
                rm.create_download_profile();
            });
	$('#regions #cancel').click(function() {
		rm.reset_all();
		return false;
	    });
        $('#regions #edit').click(function() {
                rm.edit_mode();
                $('#regions #edit').hide();
		return false;
            });
        $('#regions #clone').click(function() {
                rm.region.new_ = true;
                rm.edit_mode();
		rm.name_edit(true);
                $('#regions #edit').hide();
                $('#regions #clone').hide();
		return false;
            });
        $('#regions #curlayer').click(function() {
                $('#regions #layer').val(get_active_layer().id);
		return false;
            });
        $('#regions #curdepth').click(function() {
                $('#regions #depth').val(pos_info(map.getCenter(), map).effzoom);
		return false;
            });

        map.on('click', function(e) {
                rm.add_point(e);
            });
        shortcut.add('backspace', function() {
                rm.undo_point();
            }, {disable_in_input: true});
        shortcut.add('shift+backspace', function() {
                rm.undo_point();
            });

	$.each($('#profile pre'), function(i, e) {
		var $e = $(e);
		$e.unbind('click');
		$e.click(function() {
			var selection = window.getSelection();            
			var range = document.createRange();
			range.selectNodeContents($e[0]);
			selection.removeAllRanges();
			selection.addRange(range);
		    });
	    });

	$('#regions #name').placeholder();
	$('#regions #depth').placeholder();
    }

    this.name_edit = function(enabled, defval) {
	$('#regions #name')[enabled ? 'show' : 'hide']();
	$('#regions #namestatic')[enabled ? 'hide' : 'show']();
	if (defval != null) {
	    $('#regions #name').val(defval);
	    $('#regions #namestatic').text(defval);
	}
    }

    this.add_layer = function(layer) {
        var $o = $('<option />');
        $o.text(layer.id);
        $o.val(layer.id);
        if (!layer.downloadable) {
            $o.attr('disabled', 'true');
        }
        $('#regions #layer').append($o);
    }

    this.activate = function(reg) {
        if (reg == null) {
            this.region = {new_: true};

            $('#regions #clone').hide();
            $('#regions #edit').hide();

            this.edit_mode();
	    this.name_edit(true, '');
        } else {
            this.region = reg;
            this.rpoly = reg.poly;
            this.highlight(reg, true);

            // move to top
            map.removeLayer(this.rpoly);
            map.addLayer(this.rpoly);

	    this.name_edit(false, this.region.name);
            if (reg.readonly) {
                $('#regions #edit').hide();
            }
        }

        $('#regions #layer').val(get_active_layer().id);

        $('#regions #manage').show();
        $('#regions #select').hide();

        this.deactivate_other();
    }

    this.zoom_to_active = function() {
        map.fitBounds(this.rpoly.getBounds());
    }

    this.highlight = function(reg, on) {
        reg.poly.setStyle({color: on ? '#ff0' : '#444'});
        reg.$name.css('background-color', on ? '#fcc' : '');
    }

    this.add_region = function(reg) {
        var lls = [];
        $.each(reg.bound, function(i, coord) {
                lls.push(new L.LatLng(coord[0], coord[1]));
            });
        reg.poly = new L.Polygon(lls, {
                fill: false,
                weight: 2,
                opacity: .8,
            });
        reg.$name = $('<div />');
        reg.$name.text(reg.name);
        $('#regions #list').append(reg.$name);
	reg.$name.addClass('regchoice');
        this.highlight(reg, false);

        this.bind_events(reg);
        this.all_regions.push(reg);
        map.addLayer(reg.poly);
    }

    this.bind_events = function(reg) {
        var rm = this;

        var bind = function(e, bindfunc) {
            bindfunc(e, 'mouseover', function() { rm.highlight(reg, true); });
            bindfunc(e, 'mouseout', function() { rm.highlight(reg, false); });
            bindfunc(e, 'click', function() {
                    if (!rm.region) {
                        rm.activate(reg);
                    }
                    rm.zoom_to_active();
                });
        }
        bind(reg.poly, function(e, type, handler) { e.on(type, handler); });
        bind(reg.$name, function(e, type, handler) { e[type](handler); });
    }

    this.deactivate_other = function() {
        var rm = this;
        $.each(this.all_regions, function(i, reg) {
                if (reg == rm.region) {
                    return;
                }

                reg.poly.setStyle({opacity: .3});
                remove_handlers(reg.poly, 'click');
            });
        if (!this.region.new_) {
            var unbind = function(e, unbindfunc) {
                unbindfunc(e, 'mouseover');
                unbindfunc(e, 'mouseout');
            };
            unbind(this.region.poly, function(e, type) { remove_handlers(e, type); });
            unbind(this.region.$name, function(e, type) { e.unbind(type); });
        }
    }

    this.edit_mode = function() {
        if (this.editing) {
            return;
        }

        this.editing = true;
        if (this.rpoly) {
            map.removeLayer(this.rpoly);
        }

        this.rpoly = new RegionPoly(map, this.region.bound);
        var rm = this;
        this.rpoly.onchange = function() {
            rm.region.changed = true;
            rm.validate_region();
        }
        this.validate_region();

	$('#edithint').show()
    }

    this.validate_region = function() {
        if (this.region.changed && !this.rpoly.is_degenerate()) {
            $('#regions #submit').removeAttr('disabled');
        } else {
            $('#regions #submit').attr('disabled', 'true');
        }
    }

    this.add_point = function(e) {
        if (this.editing) {
            this.rpoly.new_point(e);
        }
    }

    this.undo_point = function() {
        if (this.editing) {
            this.rpoly.delete_active();
        }
    }

    this.create_download_profile = function() {
        var params = this.get_download_params();
        var valid = this.validate_params(params, function(field, msg) {
                alert(msg);
            });
        if (!valid) {
            return;
        }

        var profile = this.build_download_profile(params);
        this.present_profile(profile);
    }

    this.get_download_params = function() {
        return {
            name: $.trim($('#regions #name').val()),
            layer: $.trim($('#regions #layer').val()),
            depth: +($('#regions #depth').val() || 'NaN'),
            refresh: Boolean($('#regions #refresh').attr('checked')),
            update: this.region.changed && !this.region.new_,
            region: this.export(),
        };
    }

    this.validate_params = function(params, onerror) {
        var errors = false;
        var _err = function(f, m) {
            errors = true;
            onerror(f, m);
        }

        if (!params.name) {
            _err('name', 'region name is required');
        }
        var existing_names = [];
        $.each(this.all_regions, function(i, e) {
                existing_names.push(e.name);
            });
        if (this.region.new_ && existing_names.indexOf(params.name) != -1) {
            _err('name', 'region name already in use');
        }
        if (isNaN(params.depth) || params.depth != Math.floor(params.depth)) {
            _err('depth', 'depth must be an integer');
        }
        if (params.depth < 0 || params.depth > 30) {
            _err('depth', 'depth must be between 0 and 30');
        }
        return !errors;
    }

    this.build_download_profile = function(params) {
        var lines = [];
        var indent_level = 0;
        var add = function(field, value, indent) {
            var s = '';
            for (var i = 0; i < indent_level; i++) {
                s += '  ';
            }
            s += field + ':' + (value != null ? ' ' + value : '');
            lines.push(s);
            if (indent) {
                indent_level++;
            }
        }

        add('name', params.name);
        if (params.region) {
            add('region', params.region);
        }
        if (params.update) {
            add('update', 'true');
        }
        add('layers', null, true);
        add(params.layer, null, true);
        add('zoom', params.depth);
        if (params.refresh) {
            add('refresh-mode', 'always');
        }

        return lines.join('\n');
    }

    this.present_profile = function(profile) {
        $('#profile #simple').text('Saving download profile...');
        $.post('/saveprofile', profile, function(data) {
                $('#profile #simple').text(DL_COMMAND + ' ' + data);
            });

        $('#profile #literal').text('echo "\n' + profile + '\n" | ' + DL_COMMAND + ' -');

        $('#profile').dialog({
                modal: true,
                resizable: false,
                minWidth: 600,
            });
    }

    this.export = function() {
        return (this.editing ? this.rpoly.bounds_str() : null);
    }

    this.reset_all = function() {
	var all_reg = this.all_regions;
	var curpoly = this.rpoly;

	this.all_regions = [];
	this.region = null;
	this.rpoly = null;
	this.editing = false;
	
        $('#regions #manage').hide();
        $('#regions #select').show();

	$('#regions #edit').show();
	$('#regions #clone').show();
	$('#regions #depth').val('');
	$('#regions #refresh').removeAttr('checked');
	$('#regions #edithint').hide();

	if (curpoly.poly) {
	    curpoly.destroy();
	}
	$('#regions #list').empty();
	var rm = this;
	$.each(all_reg, function(i, reg) {
		map.removeLayer(reg.poly);
		$.each(['new_', 'changed', 'poly', '$name'], function(i, e) {
			delete reg[e];
		    });

		rm.add_region(reg);
	    });
	map.setZoom(DEFAULT_ZOOM);
    }

    this.mk_control = function() {
	var RegionsPanel = L.Control.extend({
		options: {
		    position: 'topright'
		},

		onAdd: function (map) {
		    return ctrl_init('#regions', function($div, div) {
			    L.DomEvent.disableClickPropagation(div);
			    L.DomEvent.addListener(div, 'mousewheel', L.DomEvent.stopPropagation);
			});
		},
	    });	
	this.init();
	return new RegionsPanel();
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
    this.onchange = function(){};

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
                r.onchange();
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
        this.onchange();
    }

    this.remove_point = function(p) {
        var i = this.find_point(p);
        this.points.splice(i, 1);
        this.vertexes.splice(i, 1);
        if (i != -1) {
            this.onchange();
        }
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

    this.destroy = function() {
	map.removeLayer(this.poly);
	$.each(this.points, function(i, p) {
		map.removeLayer(p);
	    });
    }

    this.init();
}

function get_icon(type) {
    return new L.Icon({
	    iconUrl: ICON_PATH + '/marker-icon-' + type + '.png',
	    shadowUrl: ICON_PATH + '/marker-shadow.png',
	    iconSize: new L.Point(25, 41),
	    iconAnchor: new L.Point(13, 41),
	    popupAnchor: new L.Point(0, -33),
	    shadowSize: new L.Point(41, 41)
	});
}

Waypoint = L.Marker.extend({
	options: {
	    dragPopup: true,
	},

	setIconType: function(type) {
	    var icon = (type != null ? get_icon(type) : new L.Icon.Default());
	    this.setIcon(icon);
	},

	onAdd: function(map) {
	    L.Marker.prototype.onAdd.call(this, map);

	    this.$content = $('#point-template').clone();
	    var $c = this.$content;
	    
	    this._key = this.options.name;
	    this.options.name = this.options.name || '';
	    this.options.comment = this.options.comment || '';
	    $c.find('#namestatic').text(this.options.name);
	    $c.find('#name').val(this.options.name);
	    $c.find('#descstatic').text(this.options.comment);
	    $c.find('#desc').val(this.options.comment);
	    this.set_pos_info();

	    this.edit_mode(this._key == null);
	    if (this._key == null) {
		this.onchange();
	    }

	    var wpt = this;
	    $c.find('#edit').click(function() {
		    wpt.edit_mode(true);
		});
	    $c.find('#submit').click(function() {
		    wpt.save();
		});
	    $c.find('#name').change(function() {
		    wpt.onchange();
		});
	    $c.find('#desc').change(function() {
		    wpt.onchange();
		});
	    this.on('drag', function() {
		    wpt.onchange(true);
		    wpt.set_pos_info();
		});
	    this.on('dragend', function() {
		    wpt.onchange();
		});

	    $c.find('#name').placeholder();
	    $c.find('#desc').placeholder();

	    $c.css('display', 'block');
	    this.bindPopup($c[0]);
	},

	refresh_popup: function() {
	    if (this._popup) {
		this._popup._update();
	    }
	},

	set_dragging: function(enable) {
	    this.options.draggable = enable;
	    this.dragging[enable ? 'enable' : 'disable']();
	},

	set_pos_info: function() {
	    var pos = this.getLatLng();
	    var fp = fmt_pos(pos.lat, pos.lng, WAYPOINT_PREC);
	    this.$content.find('#pos').html(fp.lat + '&nbsp;&nbsp;&nbsp;' + fp.lon);
	},

	edit_mode: function(enabled) {
	    var $c = this.$content;
	    $c.find('#namestatic')[enabled ? 'hide' : 'show']();
	    $c.find('#descstatic')[enabled ? 'hide' : 'show']();
	    $c.find('#name')[enabled ? 'show' : 'hide']();
	    $c.find('#desc')[enabled ? 'show' : 'hide']();
	    $c.find('#edit')[enabled ? 'hide' : 'show']();
	    $c.find('#submit')[enabled ? 'show' : 'hide']();
	    this.set_dragging(enabled);
	    this.refresh_popup();
	    if (!enabled) {
		this.setIconType();
	    }
	},

	onchange: function(indrag) {
	    if (!this.changed) {
		if (indrag) {
		    // changing the icon during drag causes drag to stop
		    return;
		}

		this.changed = true;
		this.setIconType('modified');
	    }
	},

	save: function() {
	    var $c = this.$content;
	    var wpt = this;

	    // get data
	    var pos = this.getLatLng();
	    var ptrunc = function(k) {
		return +(k.toFixed(WAYPOINT_PREC));
	    }
	    var payload = {
		key: this._key || '',
		name: $c.find('#name').val(),
		desc: $c.find('#desc').val(),
		lat: ptrunc(pos.lat),
		lon: ptrunc(pos.lng),
	    };

	    // validate data
	    if (!payload.name) {
		alert('waypoint name is required');
		return;
	    }

	    var onsuccess = function() {
		wpt.changed = false;
		wpt._key = payload.name;
		$c.find('#namestatic').text($c.find('#name').val());
		$c.find('#descstatic').text($c.find('#desc').val());
		wpt.edit_mode(false);
	    }

	    // process data
	    if (this.changed) {
		$.post('/savewaypoint', JSON.stringify(payload), onsuccess);
	    } else {
		onsuccess();
	    }	    
	},

    });

SearchResult = L.Marker.extend({
	options: {
	    zIndexOffset: 100,
	},

	setIconType: function() {
	    this.setIcon(get_icon(this.options.primary ? 'searchresult-primary' : 'searchresult'));
	},

	onAdd: function(map) {
	    L.Marker.prototype.onAdd.call(this, map);

	    this.setIconType();
	    if (this.options.primary) {
		this.setZIndexOffset(this.options.zIndexOffset + 100);
	    }

	    this.$content = $('<div />');
	    var $name = $('<div />');
	    $name.attr('id', 'namestatic');
	    $name.text(this.options.name);
	    this.$content.append($name);

	    var $desc = $('<div />');
	    $desc.html(this.options.desc || '');
	    this.$content.append($desc);

	    this.bindPopup(this.$content[0]);
	    if (this.options.primary) {
		this.openPopup();
	    }
	},

	bounds: function() {
	    var p = this.getLatLng();
	    var r = Math.max(this.options.radius, 300.); // the minimum radius determines the max zoom level

	    var map_w = $('#map').width();
	    var map_h = $('#map').height();
	    var mean_dim = Math.sqrt(map_w * map_h);
	    var r_x = r * map_w / mean_dim;
	    var r_y = r * map_h / mean_dim;
	    var METERS_PER_DEGREE = 2 * Math.PI * 6371009. / 360.; // hack
	    var lat_offset = r_y / METERS_PER_DEGREE;
	    var lon_offset = r_x / METERS_PER_DEGREE / Math.cos(p.lat * Math.PI / 180.);

	    return new L.LatLngBounds(new L.LatLng(p.lat - lat_offset, p.lng - lon_offset), new L.LatLng(p.lat + lat_offset, p.lng + lon_offset));
	},

    });

NewWaypoint = L.Control.extend({
	options: {
	    position: 'topright'
	},

	onAdd: function (map) {
	    return ctrl_init('#wp-panel', function($div, div) {
		    L.DomEvent.disableClickPropagation(div);	    
		    $div.find('#newwpt').click(function() {
			    wpt = new Waypoint(map.getCenter());
			    map.addLayer(wpt);

			    // hack or else popup is immediately closed
			    setTimeout(function() { wpt.openPopup(); }, 200);
			});
		    var $searchbutton = $div.find('#search');
		    $searchbutton.click(function() {
			    var query = $.trim($div.find('#searchquery').val());
			    if (!query) {
				return false;
			    }
			    
			    $searchbutton.attr('disabled', 'true');
			    $.get('/locsearch', {q: query}, function(data) {
				    $searchbutton.removeAttr('disabled');

				    if (data.status == 'success') {
					if (data.results.length == 0) {
					    alert('0 matches');
					} else {
					    show_search_results(data.results, map);
					}
				    } else {
					alert('search failed: ' + data.message);
				    }
				});
			    
			    return false;
			});
		    setTimeout(function() {
			    $div.find('#searchquery').focus();
			}, 100);
		});
	},
    });	

function show_search_results(results, map) {					
    var bounds = new L.LatLngBounds();
    $.each(results, function(i, e) {
	    if (i == 0) {
		e.primary = true;
	    }
	    var m = new SearchResult(new L.LatLng(e.lat, e.lon), e);
	    bounds.extend(m.bounds());
	    map.addLayer(m);
	});
    map.fitBounds(bounds);
}

ManagementOptions = L.Control.extend({
	options: {
	    position: 'topright'
	},

	onAdd: function (map) {
	    var mo = this;

	    var bind = function($e, init) {
		$e.click(function() {
			$e.hide();
			init();

			if ($(mo._container).find('a:visible').length == 0) {
			    map.removeControl(mo);
			}
		    });
	    }

	    return ctrl_init('#management-opts', function($div, div) {
		    L.DomEvent.disableClickPropagation(div);	    
		    bind($div.find('#region'), mo.options.enable_regions);
		    bind($div.find('#waypoint'), mo.options.enable_waypoints);
		});
	},
    });

function ctrl_init(sel, custom_init) {
    var $div = $(sel);
    $div.show();
    if (custom_init) {
	custom_init($div, $div[0]);
    }
    return $div[0];
}

function cache_layer(lyrspec, notfound) {
    return new L.TileLayer('/tile/' + lyrspec.id + '/{z}/{x},{y}', {
            errorTileUrl: notfound ? IMG_NOCACHE : null
        });
}

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
    return new L.Icon({
            iconUrl: render_icon(draw, w, h),
            shadowUrl: null,
            iconSize: new L.Point(w, h),
            iconAnchor: new L.Point(w * .5 * (anchor[0] + 1.), h * .5 * (1. - anchor[1])),
        });
}

/*
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
*/

/*
function non_cached(ctx, w, h) {
    var bands = 10;
    var ratio = 1;
    ctx.globalAlpha = .2;

    ctx.scale(w, h);
    ctx.rotate(Math.PI / 4);
    ctx.scale(Math.sqrt(2.), Math.sqrt(.5));

    ctx.fillStyle = 'black';
    ctx.fillRect(0, -1, 1, 2);

    ctx.fillStyle = 'white';
    var k = ratio / (ratio + 1);
    for (var i = 0; i <= bands; i++) {
        ctx.fillRect((i - .5 * k) / bands, -1, k / bands, 2);
    }
}
*/

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

function npad(n, pad) {
    var s = '' + n;
    while (s.length < pad) {
	s = '0' + s;
    }
    return s;
}

function fmt_ll(k, dir, pad, prec) {
    return dir[k >= 0 ? 0 : 1] + npad(Math.abs(k).toFixed(prec), prec + 1 + pad) + '\xb0';
};

function fmt_pos(lat, lon, prec) {
    return {
	lat: fmt_ll(lat, 'NS', 2, prec),
        lon: fmt_ll(lon, 'EW', 3, prec)
    };
}

function url_param(param, value) {
    var url = window.location.href;
    var params = url.substring(url.indexOf('?') + 1);

    var _ = function(s) { return '&' + s + '&'; };
    return _(params).indexOf(_(param + '=' + value)) != -1;
}

function remove_handlers(o, type) {
    var handlers = o._leaflet_events[type];
    if (handlers == null || handlers.length.length == 0) {
        console.log('warning: no handlers of type ' + type, o);
    }

    $.each(o._leaflet_events[type], function(i, e) {
            o.off(type, e.action);
        });
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

            var lc = this;
            this._map.on('zoomend', function() {
                    lc.reorder();
                });
        },

        initialize: function(layers) {
            this.overlay_types = ['source', 'cached', 'coverage'];
            this.active_layers = {}
            this.active_layer = null;

            var ov = {};
            var lc = this;
            $.each(this.overlay_types, function(i, e) {
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
                        maplayer.options.maxZoom = MAX_ZOOM;
                        lc._map.addLayer(maplayer);
                        lc.active_layers[type] = maplayer;
                        console.log('adding layer: ' + type + ' ' + active_layer.id);
                    }
                });
            this.active_layer = active_layer;
            this.reorder();

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

        reorder: function() {
            var ordered_layers = [];
            var lc = this;
            $.each(this.overlay_types, function(i, e) {
                    var lyr = lc.active_layers[e];
                    if (lyr) {
                        ordered_layers.push(lyr);
                    }
                });
            this._map.order_layers(ordered_layers);
        },

        select: function(e) {
            var inputs = $(this._form).find('input');
            var lc = this;
            $.each(inputs, function(i, input) {
                    var o = lc._layers[input.layerId];
                    if (o.layer == e || o.layer.id == e) {
                        input.checked = true;
                    }
                });
            this._onInputClick();
        }
    });

