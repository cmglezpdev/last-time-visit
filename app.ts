import { Hono } from "https://deno.land/x/hono@v4.2.1/mod.ts";
import { cors, serveStatic } from "https://deno.land/x/hono@v4.2.1/middleware.ts";
import { streamSSE } from "https://deno.land/x/hono@v4.2.1/helper/streaming/index.ts";

interface LastVisit {
    country: string;
    city: string;
    flag: string;
}

const db = await Deno.openKv()
const app = new Hono()
let i = 0;

app.use(cors())
app.get('/', serveStatic({ path: './index.html' }))

app.post('/visit', async (c) => {
    const { country, city, flag } = await c.req.json<LastVisit>()
    await db
        .atomic()
        .set(['lastVisit'], { country, city, flag })
        .sum(['visits'], 1n)
        .commit()
    return c.json({ message: 'ok' })
})

app.get('/visit', (c) => {
    return streamSSE(c, async (stream) => {
        const watcher = db.watch([['lastVisit']]);

        for await (const entry of watcher) {
            const { value } = entry[0]
            if(value != null) {
            await stream.writeSSE({
                data: JSON.stringify(value),
                event: 'update',
                id: String(i ++)
            })
            }
        }
    })
})

Deno.serve(app.fetch)