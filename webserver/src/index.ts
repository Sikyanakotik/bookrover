import express from 'express';
import path from "path";
import * as dotenv from 'dotenv';
import process from 'node:process';

dotenv.config()
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
`
const html_footer = "</body></html>"


function toTitleCase(str: string): string {
  return str
    .toLowerCase()
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}


if (!port) {
    console.log("Could not start app: WEBSERVER_PORT environment variable undefined.")
    process.exit(1);
}

app.get("/status", (req, res) => {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/html');
    res.send("Bookrover webserver is <b>online</b>.");
});


app.get("/", (req, res) => {
    res.sendFile(path.join(import.meta.dirname, '../public/index.html'));
});

app.get("/generate_loader", async (req, res) => {
    let query = req.query.query
    if (!query || typeof(query) != "string") {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No query provided`);
        return;
    }
    let query_uri = new URLSearchParams({query: query})
    res.setHeader('Content-Type', 'text/html');
    res.send(html_header +
        `
        <h1>Generating reading list...</h1>
        <h3>This could take up to a minute.</h3>
        <script>
            window.location.replace("http://${domain}:${port}/generate_list?${query_uri.toString()}")
        </script>` + html_footer
    )
})


app.get("/list_loader", async (req, res) => {
    let reading_list_id = req.query.reading_list_id
    if (!reading_list_id || typeof(reading_list_id) != "string") {
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
            window.location.replace("http://${domain}:${port}/reading_lists/${reading_list_id}")
        </script>` + html_footer
    )
})


app.get("/generate_list", async (req, res) => {
    let query = req.query.query
    if (!query) {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: No query provided`);
    }

    const response = await fetch(
        `http://${domain}:${engine_port}/reading_lists`,
        {
            method: "POST",
            mode: "cors",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({query: query})
        }
    )
    if (response && response.status === 200) {
        let response_data = await response.json();
        if ("reading_list_id" in response_data) {
            let reading_list_id: string = response_data.reading_list_id;
            res.statusCode = 200;
            res.setHeader('Content-Type', 'text/html');
            //res.send(`${html_header}<b>Query</b>: ${req.query.query}<br><b>Response: ${await response.text()}${html_footer}`);
            res.send(html_header +
                `
                <h1>Fetching reading list...</h1>
                <h3>This should be quick, unless the API server's busy.</h3>
                <script>
                    window.location.replace("http://${domain}:${port}/reading_lists/${reading_list_id}");
                </script>` + html_footer
            )
        } else {
            res.statusCode = 500;
            res.setHeader('Content-Type', 'text/html');
            res.send(`<b>Error</b>: Bad response from API server.`);
        }
    } else {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>: API server error.`);
    }
});


app.get("/reading_lists/:reading_list_id", async (req, res) => {
    let query_uri = new URLSearchParams({reading_list_id: req.params.reading_list_id})
    const response = await fetch(
        `http://${domain}:${engine_port}/reading_lists?${query_uri}`,
        {
            method: "GET",
            mode: "cors"
        }
    )
    if (response && response.status === 200) {
        let response_data = await response.json();
        if (!response_data || !("user_id" in response_data) || !("name" in response_data) 
            || !("prompt" in response_data) || !("created_at" in response_data)
            || !("books" in response_data)) {
            res.statusCode = 500;
            res.setHeader('Content-Type', 'text/html');
            res.send(`<b>Error</b>: Bad response from API server.`);
        }
        
        let html_body = `
            <h1 style="color:#0033aa">Bookrover</h1>
            <h3>RAG-powered fiction search engine</h3>
            <hr />
            <table style="width:80%">
                <tr>
                    <td id="list_name_label" contenteditable="false" colspan=2 style="font-size:2em; width=80%">
                        ${response_data.name}
                    </td>
                </tr>
                <tr>
                    <td style="font-size:0.75em">
                        <button id="list_name_edit_button">Edit name</button>
                    </td>
                    <td style="font-size:0.75em; text-align:right">
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
                                                             name: list_name_label.textContent});
                        fetch("http://${domain}:${engine_port}/reading_lists/update_name?" + query_uri, {
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
                        fetch("http://${domain}:${engine_port}/reading_lists?" + query_uri, {
                            method: "DELETE",
                            mode: "cors"
                        })
                        .then(response => {
                            console.log('Delete request sent. Server response:', response.status);
                            if (response.status === 200) {
                                window.location.replace("http://${domain}:${port}");
                            }
                        });
                    }
                });

            </script>

            <h4>Prompt: ${response_data.prompt}<br />
            Created ${response_data.created_at}</h4>
        `;
        let count: number = 1;
        for (let book of response_data.books) {
            html_body += `
            <table style="width:80%">
                <tr style="background-color:#4466FF">
                    <td colspan="2" style="text-align:left; font-size:1.5em; color:#bbddff">
                    <b>‣ ${count}</b>
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
                    <td style="text-align:left;width:20%"><b>Release date:</b> ${book.release_date}</td>
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
            `
            count++;
        }
        res.statusCode = 200;
        res.setHeader('Content-Type', 'text/html');
        res.send(`${html_header}${html_body}${html_footer}`);

    } else {
        res.statusCode = response.status;
        res.setHeader('Content-Type', 'text/html');
        res.send(`<b>Error</b>:API server error.`);
    }
});

const webserver = app.listen(port, () => {
    console.log(`Server is running at http://localhost:${port}`);
    console.log("Server listening:", webserver.address());
});
console.log("Webserver module finished setup.");