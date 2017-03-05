window.onload = function(){
    var set_auto_reload_button = document.getElementById("set_auto_reload");
    var stop_auto_reload_button = document.getElementById("stop_auto_reload");
    var isAutoReloadEnabled = location.search.substring(1);
    if(isAutoReloadEnabled){
        set_auto_reload_button.className = "pure-button pure-button-disabled";
        stop_auto_reload_button.className = "pure-button pure-button-active";
        setTimeout("location.reload()", 1000 * 10);
    }
}
function set_auto_reload(){
    alert("Auto Relode Mode Start.");
    location.search = "isAutoReloadEnabled";
}
function stop_auto_reload(){
    alert("Stop Auto Relode Mode.");
    location.search = "";
}
