
var taskHighlightColor = "#000";
var edgeHighlightColor = "#000";

var taskMenu = document.getElementById("task-menu"); 
taskMenu.childNodes.forEach(node => {
    if (node.nodeName=="LI") { 
        node.onmouseover=function() { 
            this.className+=" over"; 
        } 
        node.onmouseout=function() { 
            this.className=this.className.replace(" over", ""); 
        } 
    }
});

var dagMenu = document.getElementById("dag-menu"); 
dagMenu.childNodes.forEach(node => {
    if (node.nodeName=="LI") { 
        node.onmouseover=function() { 
            this.className+=" over"; 
        } 
        node.onmouseout=function() { 
            this.className=this.className.replace(" over", ""); 
        } 
    }
});

function getTaskByName(name) {
    for(var i=0; i<gTasks.length; i++) {
        var task = gTasks[i];
        if (task.name == name) {
            return task;
        }
    }
    return null;
};


function showError(error) {
    alert(error);
};


function log(info) {
    console.log(info);
};

function getNewTaskName() {
    let all = ["dag"];
    for (var i=0; i<gTasks.length; i++) {
        all.push(gTasks[i].name);
    }
    for (var j=all.length+1; 1>0; j++) {
        let name = "task" + j;
        if (all.indexOf(name) == -1) {
            return name;
        }
    }
};

function addNewTask(type) {
    log(">> addNewTask " + type);
    let task = {"name": null, "type": type, "upstreams": []};
    
    let curNodeType = getNodeType(gRef);
    if (curNodeType == "dag") {
        task.upstreams.push("dag");
    } else if (curNodeType == "task") {
        task.upstreams.push(gRef.name);
    } else {
        showError("no current node");
        return;
    }

    task.name = getNewTaskName();
    gTasks.push(task);
    drawDag();
    selectNode(task.name);
}

function deleteCurrentTask() {
    log(">> deleteCurrentTask");
    let idx = -1;
    for (var i=0; i<gTasks.length; i++) {
        if (gTasks[i].name == gRef.name) {
            idx = i;
            break
        }
    }
    if (idx == -1) {
        return;
    }
    let task = gTasks[idx];
    for (var i=0; i<gTasks.length; i++) {
        for (var j=0; j<gTasks[i].upstreams.length; j++) {
            if (gTasks[i].upstreams[j] == task.name) {
                gTasks[i].upstreams.splice(j, 1);
                break
            }
        }
    }
    gTasks.splice(idx, 1);
    drawDag();
    gRef = null;
    selectNode("dag");
}

function drawDag() {
    log(">> drawDag");
    gGraph = new dagreD3.graphlib.Graph();
    gGraph.setGraph({
        nodesep: 15,
        ranksep: 15,
        rankdir: "LR",
        marginx: 20,
        marginy: 20
    });
    
    var html = "<div class=dag_node>";
    html += "<span class=name>" + gDag.name + "</span>";
    html += "<span class=type>dag</span>";
    html += "</div>";
    gGraph.setNode("dag", {
        id: "node_dag",
        labelType: "html",
        label: html,
        rx: 50,
        ry: 50,
        padding: 10
    });
    
    gTasks.forEach(task => {
        var html = "<div class=task_node>";
        html += "<span class=name>" + task.name + "</span>";
        html += "<span class=type>" + task.type + "</span>";
        html += "</div>";
        gGraph.setNode(task.name, {
            id: "node_" + task.name,
            labelType: "html",
            label: html,
            rx: 5,
            ry: 5,
            padding: 10
        });

        task.upstreams.forEach(upname => {
            gGraph.setEdge(upname, task.name, {label: "", width: 40});
        });

    });

    var render = new dagreD3.render();
    var svg = d3.select("svg");
    var inner = svg.select("g");
    inner.call(render, gGraph);
    
    var zoom = d3.zoom().on("zoom", function() {
        inner.attr("transform", d3.event.transform);
        }
    );
    svg.call(zoom);
    
    var graphWidth = gGraph.graph().width + 80;
    var graphHeight = gGraph.graph().height + 40;
    var width = parseInt(svg.style("width").replace(/px/, ""));
    var height = parseInt(svg.style("height").replace(/px/, ""));
    var zoomScale = Math.min(width / graphWidth, height / graphHeight);
    if (zoomScale > 1.5) {
        zoomScale = 1.5;
    }
    var translateX = (width / 2) - ((graphWidth * zoomScale) / 2)
    var translateY = (height / 2) - ((graphHeight * zoomScale) / 2);
    svg.call(zoom.transform, d3.zoomIdentity.translate(
                translateX, translateY).scale(zoomScale));

    d3.selectAll("g.node").on("contextmenu", function(d) {
        d3.event.preventDefault();
        selectNode(d);
        taskMenu.style.display = 'none';
        dagMenu.style.display = 'none';
        let rect = this.getBoundingClientRect();
        if (d != "dag") {
            taskMenu.style.top = (rect.y + 2) + "px";
            taskMenu.style.left = (rect.x + rect.width + 5) + "px";
            taskMenu.style.display = "block";
        } else {
            dagMenu.style.top = (rect.y + 2) + "px";
            dagMenu.style.left = (rect.x + rect.width + 5) + "px";
            dagMenu.style.display = "block";
        }
    });

    d3.selectAll("g.node").on("click", function(d) {
        selectNode(d);
    });
    
    d3.selectAll("g.node").on("mouseover", function(d) {
        d3.select(this).selectAll("rect").style("stroke", taskHighlightColor);
    });
    
    d3.selectAll("g.node").on("mouseout", function(d){
        d3.select(this).selectAll("rect").style("stroke", null);
    });

    d3.selectAll("g.edgePath").on("mouseover", function(d) {
        d3.select(this).selectAll("path").style("stroke", edgeHighlightColor);
    });
    
    d3.selectAll("g.edgePath").on("mouseout", function(d){
        d3.select(this).selectAll("path").style("stroke", null);
    });
};

function fillInfoData(id, data) {
    log(">> fillInfoData " + id);
    $("#" + id).find("[name]").each(function() {
        let name = $(this).attr("name");
        let type = $(this).attr("type");
        let value = data.hasOwnProperty(name)? data[name] : '';
        if (type == "text") {
            $(this).val(value);
        } else if (type == "editor") {
            var editor = ace.edit($(this).attr("id"));
            editor.setValue(value, 1);
        } else if (type == "select") {
            $(this).val(value);
        } else if (type == "select2") {
            if (name == "upstreams") {
                let all = [{"id": "dag", "text": "dag"}];
                for (var i=0; i<gTasks.length; i++) {
                    if (gTasks[i].name != data.name) {
                        all.push({"id": gTasks[i].name, "text": gTasks[i].name});
                    }
                }
                $(this).find("#s2:first").select2({
                    tags: true,
                    data: all, 
                    multiple: true}
                    ).val(value.join(",")).trigger("change");
            }
        }
    });
};

function readInfoData(id) {
    log("readInfoData >>> " + id);
    var data = {};
    $("#" + id).find("[name]").each(function() {
        let name = $(this).attr("name");
        let type = $(this).attr("type");
        if (type == "text") {
            data[name] = $(this).val();
        } else if (type == "editor") {
            var editor = ace.edit($(this).attr("id"));
            data[name] = editor.getValue();
        } else if (type == "select") {
            data[name] = $(this).val();
        } else if (type == "select2") {
            let a = $(this).find("#s2:first").val().split(",");
            data[name] = []
            for (var i=0; i<a.length; i++) {
                if (!isEmpty(a[i])) {
                data[name].push(a[i]);
                }
            }
        }
    });
    log(data);
    return data;
};

function getNodeType(data) {
    if (isEmpty(data)) {
        return null;
    } else if (data.hasOwnProperty("upstreams")) {
        return "task";
    } else {
        return "dag";
    }
};

function getCurrentInfoId() {
    console.log("getCurrentInfoId >>>");
    let type = getNodeType(gRef);
    if (type == null) {
        return null;
    } else if (type == "dag") {
        return "dag-info";
    } else {
        return gRef.type + "-task-info";
    }
};

function isEmpty(obj) {
    if(typeof obj == "undefined" || obj == null || obj == "") {
        return true;
    }
    return false;
};

function checkDag() {
    var reName = /^[_a-z]+[_a-z0-9]*$/i;
    var reCrond = /^(\s*\*(\/\d+)?\s+){5}$/;
    if (! reName.test(gDag.name)) {
        return 'dag 命名错误';
    }
    if (! reCrond.test(gDag.crond + ' ')) {
        return 'dag 调度时间设置无效';
    }
    if (gDag.end_date < gDag.start_date) {
        return 'dag 结束时间不能早于开始时间';
    }
    for (var i=0; i<gTasks.length; i++) {
        var task = gTasks[i];
        var err = checkTask(task);
        if (!isEmpty(err)) {
            return err;
        }
    }
};

function getDownstreams(task) {
    console.log(">> getDownstreams ");
    var ds = [];
    for (var i=0; i<gTasks.length; i++) {
        for (var j=0; j<gTasks[i].upstreams.length; j++) {
            let up = gTasks[i].upstreams[j];
            if (up == task) {
                let subs = getDownstreams(gTasks[i].name);
                for (var k=0; k<subs.length; k++) {
                    ds.push(subs[k]);
                }
                ds.push(gTasks[i].name);
            }
        }
    }
    return ds;
};

function checkTask(task) {
    var reName = /^[_a-z]+[_a-z0-9]*$/i;
    if (! reName.test(task.name)) {
        return task.name + '命名错误';
    }
    
    if (task.upstreams.length <= 0) {
        return task.name + "没有上游任务";
    }
    
    let all = ["dag"];
    for (var i=0; i<gTasks.length; i++) {
        if (gTasks[i].name != task.name) {
            all.push(gTasks[i].name);
        }
    }

    let downs = getDownstreams(task.name);
    for (var i=0; i<task.upstreams.length; i++) {
        let up = task.upstreams[i]; 
        if(all.indexOf(up) == -1) {
            return task.name + "上游任务 " + up + " 不存在";
        }
        if(downs.indexOf(up) > -1) {
            return task.name + "上游任务 " + up + " 和当前任务存在环路";
        }
    }
    if (task.type == "sql") {
        if (isEmpty(task.conn_id)) {
            return task.name + "数据源不能为空";
        }
    } else if (task.type == "upload") {
        if (isEmpty(task.conn_id)) {
            return task.name + "数据源不能为空";
        } else if (isEmpty(task.table)) {
            return task.name + "表名不能为空";
        } else if (isEmpty(task.file)) {
            return task.name + "没有选择文件";
        }
    } else if (task.type == "copy") {
        if (isEmpty(task.src_conn_id)) {
            return task.name + "数据源不能为空";
        } else if (isEmpty(task.dst_conn_id)) {
            return task.name + "目标数据源不能为空";
        } else if (isEmpty(task.dst_table)) {
            return task.name + "目标表不能为空";
        }
    }
    return "";
};

function saveInfoData() {
    log(">> saveInfoData");
    var data = readInfoData(getCurrentInfoId());
    if (!isEmpty(data) && !isEmpty(gRef)) {
        Object.keys(data).forEach(function(key) {
            gRef[key] = data[key];
        });
    }
    updateDag();
    drawDag();
};

function selectNode(name) {
    log(">> selectNode " + name);
    // save cur data
    var data = readInfoData(getCurrentInfoId());
    if (!isEmpty(data) && !isEmpty(gRef)) {
        Object.keys(data).forEach(function(key) {
            gRef[key] = data[key];
        });
        drawDag();
    }

    if (name == "dag") {
        gRef = gDag;
    } else {
        gRef = getTaskByName(name);
    }
    clearNodesClass();
    changeNodesClass([name], "active");
    changeNodesClass(gGraph.predecessors(name), "upstream");
    changeNodesClass(gGraph.successors(name), "downstream");
    showNodeInfo();
};

function clearNodesClass() {
    d3.selectAll('.node').each(function(d) {
        $(this).attr("class", "node enter");
    });
};

function changeNodesClass(nodes, clsName) {
    nodes.forEach (function (id) {
        $('#node_' + id).attr("class", "node enter " + clsName);
    });
};

function showNodeInfo() {
    var id = getCurrentInfoId();
    fillInfoData(id, gRef);
    $(".info-container .info").css("display", "none");
    $(".info-container .info").each(function() {
        let cls = $(this).attr("class");
        $(this).attr("class", cls.replace("active", ""));
    }); 
    $("#" + id).css("display", "flex");
    let cls = $("#" + id).attr("class");
    $("#" + id).attr("class", cls + " active");
};

function updateDag() {
    var err = checkDag();
    if (!isEmpty(err)) {
        showError(err);
        return;
    }

    var data = {"dag": gDag};
    $.ajax({
        url: 'update_dag?',
        type: 'POST',
        contentType: "application/json; charset=utf-8",
        data: JSON.stringify(data),
        success: function(resp) {
            if (resp.code != "SUCCESS") {
                showError(resp.desc);
            } else {
                if (isEmpty(gDag.uuid)) {
                    gDag.uuid = resp.data.uuid;
                }
            }
        },
        error: function() {
            showError("更新dag失败");
        }
    });
};

function onLoad() {
    drawDag();
    selectNode("dag");
};

document.addEventListener("DOMContentLoaded", onLoad);
document.addEventListener("click", (event) => {
    taskMenu.style.display = 'none';
    dagMenu.style.display = 'none';
});

