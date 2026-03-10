import express, { response } from 'express';
import * as dotenv from 'dotenv';
import process from 'node:process';
dotenv.config();
const app = express();
const domain = process.env.BOOKROVER_DOMAIN;
const port = process.env.WEBSERVER_PORT;
const engine_port = process.env.ENGINE_PORT;
const html_header = `
<!DOCTYPE html>
<html>
    <head>
       <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1" />
        <title>Bookrover</title>
    </head>
    <body>
        <a href="/" style="text-decoration: none;"><h1 style="color:#0033aa">Bookrover</h1></a>
        <h3>RAG-powered fiction search engine</h3>
        <hr />
`;
const html_footer = "</body></html>";
function toTitleCase(str) {
    return str
        .toLowerCase()
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}
function formatDate(date_string) {
    let year_string = date_string.slice(0, 4);
    let month_string = date_string.slice(5, 7);
    let day_string = date_string.slice(8, 10);
    let month = ["January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November",
        "December"][Number(month_string) - 1];
    return `${month} ${Number(day_string)}, ${Number(year_string)}`;
}
if (!port) {
    console.log("Could not start app: WEBSERVER_PORT environment variable undefined.");
    process.exit(1);
}
app.get("/status", (req, res) => {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/html');
    res.send("Bookrover webserver is <b>online</b>.");
});
app.get("/", async (req, res) => {
    // res.sendFile(path.join(import.meta.dirname, '../public/index.html'));
    const response = await fetch(`http://${domain}:${engine_port}/reading_lists`, {
        method: "GET",
        mode: "cors"
    });
    if (response.status !== 200) {
        res.statusCode = response.status;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No or bad reponse from API server.`);
        return;
    }
    let html_index_form = `
        <h3 style="text-align: center;">Generate reading lists from plaintext prompts</h3>
        <form action="/generate_loader" style="text-align: center;">
        <label for="query">Query</label>
        <input type="text" id="query" name="query" style="width: 70%">
        <input type="submit" value="Submit">
        </form>
        <br />
    `;
    let response_data = await response.json();
    if ((!response_data) || !("lists" in response_data)) {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: Bad reponse from API server.`);
        return;
    }
    let html_index_list_list = "";
    if (response_data.lists.length > 0) {
        html_index_list_list = `
        <hr />
        <table style="width: 80%; margin-left: auto; margin-right: auto;">
            <tr style="background-color:#4466FF">
                <td colspan=2 style="text-align:left; width:20%; font-size:1.5em; color:#bbddff">
                    ‣ My reading lists
                </td>
            </tr>
            <tr>
                <td style="text-align:left;"><b>Name</b></td>
                <td style="text-align:right;"><b>Created</b></td>
            </tr>   
        `;
        for (let reading_list of response_data.lists) {
            html_index_list_list += `
            <tr>
                <td style="text-align:left;"><a href="/reading_lists/${reading_list.id}">${reading_list.name}</a></td>
                <td style="text-align:right;"><a href="/reading_lists/${reading_list.id}">${formatDate(reading_list.created_at)}</a></td>
            </tr>
            `;
        }
        html_index_list_list += "</table>";
    }
    res.setHeader('Content-Type', 'text/html');
    res.send(`${html_header}${html_index_form}${html_index_list_list}${html_footer}`);
});
app.get("/generate_loader", async (req, res) => {
    let query = req.query.query;
    if (!query || typeof (query) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No query provided`);
        return;
    }
    let extend_list_id = req.query.extend_list_id;
    let query_uri;
    if (!extend_list_id || typeof (extend_list_id) != "string") {
        query_uri = new URLSearchParams({ query: query });
    }
    else {
        query_uri = new URLSearchParams({
            query: query,
            extend_list_id: extend_list_id
        });
    }
    res.setHeader('Content-Type', 'text/html');
    res.send(html_header +
        `
        <h1>Generating reading list...</h1>
        <h3>This could take up to a minute.</h3>
        <script>
            window.location.replace("/generate_list?${query_uri.toString()}")
        </script>` + html_footer);
});
app.get("/list_loader", async (req, res) => {
    let reading_list_id = req.query.reading_list_id;
    if (!reading_list_id || typeof (reading_list_id) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No query provided`);
        return;
    }
    res.setHeader('Content-Type', 'text/html');
    res.send(html_header +
        `
        <h1>Fetching reading list...</h1>
        <h3>This should be quick, unless the API server's busy.</h3>
        <script>
            window.location.replace("../reading_lists/${reading_list_id}")
        </script>` + html_footer);
});
app.get("/generate_list", async (req, res) => {
    let body;
    let query = req.query.query;
    if (!query || typeof (query) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No query provided`);
        return;
    }
    let extend_list_id = req.query.extend_list_id;
    if (!extend_list_id || typeof (extend_list_id) != "string") {
        body = JSON.stringify({ query: query });
    }
    else {
        body = JSON.stringify({
            query: query,
            extend_list_id: extend_list_id
        });
    }
    const response = await fetch(`http://${domain}:${engine_port}/reading_lists`, {
        method: "POST",
        mode: "cors",
        headers: {
            "Content-Type": "application/json"
        },
        body: body
    });
    if (response && response.status === 200) {
        let response_data = await response.json();
        if ("reading_list_id" in response_data) {
            let reading_list_id = response_data.reading_list_id;
            res.statusCode = 200;
            res.setHeader('Content-Type', 'text/html');
            //res.send(`${html_header}<b>Query</b>: ${req.query.query}<br><b>Response: ${await response.text()}${html_footer}`);
            res.send(html_header +
                `
                <h1>Fetching reading list...</h1>
                <h3>This should be quick, unless the API server's busy.</h3>
                <script>
                    window.location.replace("../reading_lists/${reading_list_id}");
                </script>` + html_footer);
        }
        else {
            res.statusCode = 500;
            res.setHeader('Content-Type', 'text/html');
            res.send(`<b>Error</b>: Bad response from API server.`);
        }
    }
    else {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: API server error.`);
    }
});
app.get("/reading_lists/:reading_list_id", async (req, res) => {
    let query_uri = new URLSearchParams({ reading_list_id: req.params.reading_list_id });
    const response = await fetch(`http://${domain}:${engine_port}/reading_lists?${query_uri}`, {
        method: "GET",
        mode: "cors"
    });
    if (response && response.status === 200) {
        let response_data = await response.json();
        if (!response_data || !("user_id" in response_data) || !("name" in response_data)
            || !("prompt" in response_data) || !("created_at" in response_data)
            || !("books" in response_data)) {
            res.statusCode = 500;
            res.setHeader('Content-Type', 'text/html');
            res.send(`<b>Error</b>: Bad response from API server.`);
        }
        let extend_query_uri = new URLSearchParams({
            query: response_data.prompt,
            extend_list_id: req.params.reading_list_id
        });
        let html_body = `
            <table style="width:80%; border: 0px">
                <tr>
                    <td id="list_name_label" contenteditable="false" colspan=2 style="font-size:2em; width=80%">
                        ${response_data.name}
                    </td>
                </tr>
                <tr>
                    <td>
                        <button id="list_name_edit_button">Edit name</button>
                    </td>
                    <td style="text-align:right">
                        <button id="list_delete_button">Delete list</button>
                    </td>
                </tr>
            </table>

            <script type="text/javascript">
                const list_name_label = document.getElementById('list_name_label');
                const list_name_edit_button = document.getElementById('list_name_edit_button');
                const list_delete_button = document.getElementById('list_delete_button');

                list_name_edit_button.addEventListener('click', () => {
                    if (list_name_edit_button.textContent === 'Edit name') {
                        list_name_label.setAttribute('contenteditable', true);
                        list_name_edit_button.textContent = 'Save name';
                        list_name_label.focus();
                    } else {
                        list_name_label.setAttribute('contenteditable', false);
                        list_name_edit_button.textContent = 'Edit name';
                        let query_uri = new URLSearchParams({reading_list_id: '${req.params.reading_list_id}',
                                                             name: list_name_label.textContent.trim()});
                        fetch("http://${domain}:${port}/reading_lists/update_name?" + query_uri, {
                            method: "PUT",
                            mode: "cors"
                        })
                        .then(response => {
                            console.log('Name change request sent. Server response:', response.status);
                        });
                    }
                });

                list_name_label.addEventListener('keydown', (event) => {
                    if (event.key === 'Enter' && list_name_label.getAttribute('contenteditable') === 'true') {
                        event.preventDefault();
                        list_name_edit_button.click();
                    }
                });

                list_delete_button.addEventListener('click', () => {
                    if (confirm("Delete this reading list? This cannot be undone.")) {
                        let query_uri = new URLSearchParams({reading_list_id: '${req.params.reading_list_id}'});
                        fetch("http://${domain}:${port}/reading_lists?" + query_uri, {
                            method: "DELETE",
                            mode: "cors"
                        }).then(response => {
                            window.location.replace("..");
                        });
                    }
                });

            </script>

            <h4>Prompt: ${response_data.prompt}<br />
            Created ${response_data.created_at}</h4>
            <button id="extend_list_top_button" onclick="window.location.href='../generate_loader?${extend_query_uri.toString()}'">Extend list</button>
            <br />
        `;
        let count = 1;
        for (let book of response_data.books) {
            html_body += `
            <table style="width:80%; border:2px solid #4466FF">
                <tr style="background-color:#4466FF">
                    <td style="text-align:left; width:20%; font-size:1.5em; color:#bbddff">
                    <b>‣ ${count}</b>
                    </td>
                    <td style="text-align:right">
                        <button id="remove_book_button_${book.id}">Remove book</button>
                    </td>
                </tr>
                <tr>
                    <td colspan="2" style="text-align:left; font-size:1.25em">
                    <b>${book.title}</b>
                    </td>
                </tr>
                <tr>
                    <td colspan="2" style="text-align:left">
                    <b>${book.authors.join(", ")}</b>
                    </td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>Release date:</b> ${formatDate(book.release_date)}</td>
                    <td rowspan="7" style="text-align:left">
                        <b>Description</b><p>${book.description}</p>
                    </td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>Popularity score:</b> ${book.num_good_ratings}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>User rating / 5.0:</b> ${book.average_rating.toFixed(2)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>Genres:</b> ${book.genre_tags.length ? toTitleCase(book.genre_tags.join(", ")) : "None"}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>Moods:</b> ${book.mood_tags.length ? toTitleCase(book.mood_tags.join(", ")) : "None"}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>Content warnings:</b> ${book.content_tags.length ? toTitleCase(book.content_tags.join(", ")) : "None"}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><b>ISBN:</b> ${book.isbn_13}</td>
                </tr>
            </table><br />

            <script type="text/javascript">
                const remove_book_button_${book.id} = document.getElementById('remove_book_button_${book.id}');

                remove_book_button_${book.id}.addEventListener('click', () => {
                    if (confirm("Remove ${book.title} from this reading list? It will not be recommended again for this list. This cannot be undone.")) {
                        let query_uri = new URLSearchParams({reading_list_id: '${req.params.reading_list_id}',
                                                             book_id: '${book.id}'});
                        fetch("http://${domain}:${port}/reading_lists/book?" + query_uri, {
                            method: "DELETE",
                            mode: "cors"
                        }).then(response => {
                            window.location.reload();
                        });
                    }
                });
            </script>

            `;
            count++;
        }
        html_body += `
            <button id="extend_list_bottom_button" onclick="window.location.href='../generate_loader?${extend_query_uri.toString()}'">Extend list</button>
        `;
        res.statusCode = 200;
        res.setHeader('Content-Type', 'text/html');
        res.send(`${html_header}${html_body}${html_footer}`);
    }
    else {
        res.statusCode = response.status;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>:API server error.`);
    }
});
app.put("/reading_lists/update_name", async (req, res) => {
    let reading_list_id = req.query.reading_list_id;
    if (!reading_list_id || typeof (reading_list_id) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No reading list ID provided`);
        return;
    }
    let name = req.query.name;
    if (!name || typeof (name) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No name provided`);
        return;
    }
    let query_uri = new URLSearchParams({ reading_list_id: reading_list_id,
        name: name });
    fetch(`http://${domain}:${engine_port}/reading_lists/update_name?` + query_uri, {
        method: "PUT",
        mode: "cors"
    }).then(response => {
        console.log('Name change request sent. Server response:', response.status);
    });
    res.statusCode = response.statusCode;
    res.send();
});
app.delete("/reading_lists", async (req, res) => {
    let reading_list_id = req.query.reading_list_id;
    if (!reading_list_id || typeof (reading_list_id) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No reading list ID provided`);
        return;
    }
    let query_uri = new URLSearchParams({ reading_list_id: reading_list_id });
    fetch(`http://${domain}:${engine_port}/reading_lists?` + query_uri, {
        method: "DELETE",
        mode: "cors"
    }).then(response => {
        console.log('Deletion request sent. Server response:', response.status);
    });
    res.statusCode = response.statusCode;
    res.send();
});
app.delete("/reading_lists/book", async (req, res) => {
    let reading_list_id = req.query.reading_list_id;
    if (!reading_list_id || typeof (reading_list_id) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No reading list ID provided`);
        return;
    }
    let book_id = req.query.book_id;
    if (!book_id || typeof (book_id) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No book ID provided`);
        return;
    }
    let query_uri = new URLSearchParams({ reading_list_id: reading_list_id,
        book_id: book_id });
    fetch(`http://${domain}:${engine_port}/reading_lists/book?` + query_uri, {
        method: "DELETE",
        mode: "cors"
    }).then(response => {
        console.log('Book removal request sent. Server response:', response.status);
    });
    res.statusCode = response.statusCode;
    res.send();
});
const webserver = app.listen(port, () => {
    console.log(`Server is running at http://localhost:${port}`);
    console.log("Server listening:", webserver.address());
});
console.log("Webserver module finished setup.");
