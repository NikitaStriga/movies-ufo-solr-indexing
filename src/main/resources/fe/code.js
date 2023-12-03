const postsContainer = document.querySelector(".posts-container");
const searchDisplay = document.querySelector(".search-display");
const core = "test";
const url = `http://localhost:8983/solr/${core}/select`;

const handleSearchPosts = (query) => {
    const searchQuery = query.trim().toLowerCase();

    if (searchQuery.length <= 1) {
        resetPosts()
        return
    }

    solrFetch(query)
        .then(x =>
        {
            let result = '';
            x.response.docs.forEach(el => result = result + "<div>" + el.title + "</div>");
            postsContainer.innerHTML=result;
            if (result === '')
            {
                search.style.color = "red";
            }
            else
            {
                search.style.color = "";
            }
        });

    // if (searchResults.length == 0) {
    //     searchDisplay.innerHTML = "No results found"
    // } else if (searchResults.length == 1) {
    //     searchDisplay.innerHTML = `1 result found for your query: ${query}`
    // } else {
    //     searchDisplay.innerHTML = `${searchResults.length} results found for your query: ${query}`
    // }
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
        debounce(() => handleSearchPosts(query), 50);
    },
    false
);