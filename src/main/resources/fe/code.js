const postsContainer = document.querySelector(".posts-container");
const searchDisplay = document.querySelector(".search-display");
const core = "movies";
const request_handler = "suggest_topic"
const url = `http://localhost:8983/solr/${core}/${request_handler}`;

const handleSearchPosts = (query) => {
    const searchQuery = query.trim().toLowerCase();

    console.log("vavd")
    if (searchQuery.length <= 2) {
        resetPosts()
        return
    }

    solrFetch(query)
        .then(x =>
        {
            let result = '';
            x.response.docs.forEach(el => result = result + "<div>" + el.title + "</div>");
            if (result === '')
            {
                search.style.color = "red";
                postsContainer.innerHTML="<pre>" + JSON.stringify(x, null, 3) + "</pre>"
                postsContainer.style.color= "red";
            }
            else
            {
                result = result + "<br>" + "<div style='color: red'>" + "Was found:" + x.response.numFound + "</div>";
                postsContainer.innerHTML=result;
                search.style.color = "";
                postsContainer.style.color = "";
            }
        });
};

const solrFetch = async (query) => {
    let requestUrl = url + "?q=" + query;
    let response = await fetch(requestUrl);

    return await response.json(); // сразу же преобразует в объект
}

const resetPosts = () => {
    searchDisplay.innerHTML = ""
    postsContainer.innerHTML = "";
    search.style.color = "";
};

const search = document.getElementById("search");

let debounceTimer;
const debounce = (callback, time) => {
    window.clearTimeout(debounceTimer);
    debounceTimer = window.setTimeout(callback, time);
};

search.addEventListener(
    "input",
    (event) => {
        const query = event.target.value;
        debounce(() => handleSearchPosts(query), 350);
    },
    false
);

// RELOAD core
// http://localhost:8983/solr/admin/cores?action=RELOAD&core=movies