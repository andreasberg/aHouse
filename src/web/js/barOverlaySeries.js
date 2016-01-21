define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.baroverlay = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear(),
            yValue = 0,
            css = '';

        var line = d3.svg.line()
            .x(function (d) {
                return d.x;
            })
            .y(function (d) {
                return d.y;
            });
 
/*        var miniYAxis = d3.svg.axis()
            .scale(miniScale)
            .orient('left')
            .ticks(3);
*/
        var baroverlay = function (selection) {
            
/*            var axgrp = d3.select(this).append('g')
                .classed('mini-axes',true);
*/
            selection.each(function (data) {

                var barWidth = Math.floor(xScale(new Date(8.64e7)))-xScale(new Date(0));

                var axes = d3.select(this).selectAll('.bar-y-axis')
                    .data(data, function(d){ 
                        return d.date;
                    });

//                console.log(d);


                var axEnter = axes.enter().append('g')
                    .classed('bar-y-axis',true)
                    .attr('date', function(d) {
                        return d.date;
                    })
                    .append('path');

                axes.exit().remove();
                // Draw mini y-axes on rectangles (non dynamic)
//                miniyEnter.append('path');

//                var miniy = bar.selectAll('g.mini-y-axis');
                    //.attr('style','opacity:0;'); // hide axes to start

                    //.attr('transform',function(d) { return 'translate(' +(xScale(d.date)+rectangleWidth/2)+ ',' +yScale(d.temp_max)+ ')';});
                
                axes.selectAll('path')
                    .attr('d', function(d){
                        return line([
                            { x: (xScale(d.date)+barWidth), y: yScale(d.temp_max) },
                            { x: (xScale(d.date)+barWidth), y: yScale(d.temp_min) }
                        ]);
                    });

                axes.each(function(d){
                    var my = d3.select(this);
                    var tick = my.selectAll('.tick')
                        .data(
                            function(d) { return tickData(d); },
                            function(d) { return d.tick; });

                    var tickEnter = tick.enter()
                        .append('g')
                        .classed('tick',true)
                        .attr('value',function(d) { return d.tick;});

                    tickEnter.append('line')
                        .attr('x2','-3')
                        .attr('y2','0');
                    tickEnter.append('text')
                        .attr({
                            'dy':'.35em',
                            'x':'-5',
                            'y':'0',
                            'style':'text-anchor: end;opacity: 0.5;'
                        })
                        .text(function(d) { return d.tick;});
                    tickEnter.append('line')
                        .classed('minitempline',true)
                        .attr({
                            'x1':'-12',
                            'y1':'0',
                            'y2':'0',
                            'style':'opacity:0.2;'
                        });

                    tick.exit().remove();

                    my.selectAll('.tick')
                        .attr('transform',function(d) { 
                            return 'translate(' + (xScale(d.date)+barWidth) + ',' + yScale(d.tick) + ')'; })
                        .selectAll('line.minitempline')
                            .attr('x2',-barWidth);
                    

                });
/*

                d3.select('.detailTemp').selectAll('.linesegment').remove();
                linesegments = d3.select('.detailTemp').selectAll('.linesegment')
                    .data(Object.keys(ds)); 

                linesegments.enter().append('g')
                    .classed('linesegment', true)
                    .attr("id", function(d) { return 'linesegment'+d; })
                    .append('path');

                linesegments.exit().remove();
                
                Object.keys(ds).forEach(function(key) {
                    //console.log('key '+key);
                    var line = d3.svg.line()
                        .x(function(d) {return xScale(d.date); })
                        .y(function(d) {return yScale(d.temp); });

                    //console.log(data2);
                    var selector = "g#linesegment"+key+" > path";
                    //console.log(selector);
                    var p = d3.selectAll(selector)
                        .classed({
                         'detailTemp': true,
                         'detailLine': true,
                        })
                        .attr('d',line(ds[key]));
*/
//                });
            });
        };

        baroverlay.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return baroverlay;
        };

        baroverlay.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return baroverlay;
        };

        return baroverlay;
    };

    function tickData(d) {
        // create a simple array 
        var data = [];
        var arr = arrBetween(Math.ceil(d.temp_min),Math.floor(d.temp_max));
        for (var i in arr) {
          data.push({ 'tick': arr[i], 'date': d.date});  
        } 
        //console.log(data);
        return data;
    }

    function arrBetween(lo,hi) {
        var arr = [],
        c = hi - lo + 1;
        while ( c-- ) {
            arr[c] = hi--
        }
        return arr;
    }
});


/*
// Draw mini y-axes on rectangles (non dynamic)
                miniyEnter.append('path');

                var miniy = bar.selectAll('g.mini-y-axis');
                    //.attr('style','opacity:0;'); // hide axes to start

                    //.attr('transform',function(d) { return 'translate(' +(xScale(d.date)+rectangleWidth/2)+ ',' +yScale(d.temp_max)+ ')';});
                miniy.selectAll('path')
                    .attr('d', function(d){
                        return line([
                            { x: (xScale(d.date)+rectangleWidth), y: yScale(d.temp_max) },
                            { x: (xScale(d.date)+rectangleWidth), y: yScale(d.temp_min) }
                        ]);
                    });

                miniy.each(function(d){
                    var my = d3.select(this);
                    var tick = my.selectAll('.tick')
                        .data(
                            function(d) { return tickData(d); },
                            function(d) { return d.tick; });

                    var tickEnter = tick.enter()
                        .append('g')
                        .classed('tick',true)
                        .attr('value',function(d) { return d.tick;});

                    tickEnter.append('line')
                        .attr('x2','-3')
                        .attr('y2','0');
                    tickEnter.append('text')
                        .attr({
                            'dy':'.35em',
                            'x':'-5',
                            'y':'0',
                            'style':'text-anchor: end;opacity: 0.5;'
                        })
                        .text(function(d) { return d.tick;});
                    tickEnter.append('line')
                        .classed('minitempline',true)
                        .attr({
                            'x1':'-12',
                            'y1':'0',
                            'y2':'0',
                            'style':'opacity:0.2;'
                        });

                    tick.exit().remove();

                    my.selectAll('.tick')
                        .attr('transform',function(d) { 
                            return 'translate(' + (xScale(d.date)+rectangleWidth) + ',' + yScale(d.tick) + ')'; })
                        .selectAll('line.minitempline')
                            .attr('x2',-rectangleWidth);
                    

                });
*/
//            console.log(selection);
            //selection.sort(function (d) {return d.date;});

//            console.log(Object.keys(selection));