origins = [
    "https://kxml-5n2a85vzg-kiligs-projects-7cfc26f2.vercel.app",
    "https://kxml.vercel.app",
    "http://localhost:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
