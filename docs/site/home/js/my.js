 $( document ).ready(function() {
                new WOW().init();
            });


$(window).on('scroll', function () {
      var scroll = $(window).scrollTop();
      if (scroll < 40) {
        $(".header").removeClass("sticky-bar");
      } else {
        $(".header").addClass("sticky-bar");
      }
    });
$(function(){
  $("#header").load("header.html"); 
  $("#footer").load("footer.html"); 
})

        $("#dropdownArchives").change(function(){
            var selectedVersionText = this.options[this.selectedIndex].text;

            var pattern = /(\d*\.\d*\..*) \((.*)\)/;
            var selectedVersion = pattern.exec(selectedVersionText);
            var version = selectedVersion[1];
            var releaseDate = selectedVersion[2];
            var versionHyphened = version.replace(/\./g, "-");
            var versionUnderscored = version.replace(/\./g, "_");

            $("#archiveVersion").html(version);
            $("#archiveReleaseDate").html(releaseDate);
            $("#archiveReleaseNotes").attr("href", "https://github.com/apache/tinkerpop/blob/master/CHANGELOG.asciidoc#release-" + versionHyphened);
            $("#archiveDocs").attr("href", "https://tinkerpop.apache.org/docs/" + version);
            $("#archiveContributors").attr("data-bs-target", "#contributors-" + versionUnderscored);

            var versionsWithOldNaming = ["3.2.1", "3.1.3", "3.2.0-incubating", "3.1.2-incubating", "3.1.1-incubating",
                                         "3.1.0-incubating", "3.0.2-incubating", "3.0.1-incubating", "3.0.0-incubating"];
            var consoleFileName = "apache-tinkerpop-gremlin-console-";
            var serverFileName = "apache-tinkerpop-gremlin-server-";
            if (versionsWithOldNaming.includes(version)) {
                consoleFileName = "apache-gremlin-console-";
                serverFileName = "apache-gremlin-server-";
            }

            var incubatingVersion = version.endsWith("-incubating");
            var archiveUrl = incubatingVersion ? "https://archive.apache.org/dist/incubator/tinkerpop/" : "https://archive.apache.org/dist/tinkerpop/";

            $("#archiveDownloadConsole").attr("href", archiveUrl + version + "/" + consoleFileName + version + "-bin.zip");
            $("#archiveDownloadServer").attr("href", archiveUrl + version + "/" + serverFileName + version + "-bin.zip");
            $("#archiveDownloadSource").attr("href", archiveUrl + version + "/apache-tinkerpop-" + version + "-src.zip");

            var versionsWithoutUpgradeDocs = ["3.1.0-incubating", "3.0.2-incubating", "3.0.1-incubating", "3.0.0-incubating"];
            if (versionsWithoutUpgradeDocs.includes(version)) {
                $("#archiveUpgrade a").attr("href", "#");
                $("#archiveUpgrade").hide();
            } else {
                $("#archiveUpgrade a").attr("href", "https://tinkerpop.apache.org/docs/" + version + "/upgrade/#_tinkerpop_" + versionUnderscored);
                $("#archiveUpgrade").show();
            }

        });

        $("#dropdownArchives").change()
