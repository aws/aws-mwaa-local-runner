# canvas_page_views

This dag uses the `api/v1/users/{user_id}/page_views` endpoint.

### Handling high request counts

Collecting canvas page views comes with a heavy request burden. 

**The request sequence:**
1. `users/{user_id}/page_views` to collect the students page views
2. For each page view, `course/{course_id}` to collect the assignments course name
3. For each page view, `course/{course_id}/assignments/{assignment_id}` to collect the assignment name

It isn't common, but students _can_ have >100 page views in a day so depending on the amount of student activity, this etl can quickly get out of hand. 

**The following measures are used to manage the high request burden**
1. While iterating over the student ids, an archive object is used to store all the data for their page views. For each assignment, we first check if the assignment has been stored in the archive. If it is, we collect the assignment's data from the archive and do not make requests #2 and #3. 
2. The student ids are split into chunks and parallel Python operators are run that pull their assigned chunks from s3, and collect the page views for their assigned ids. 
    - **This requires api tokens** for each parallel operator. 
    - The number of parallel operators cannot exceed the number of tokens stored in the environment. 
    - Each token is formatted as `CANVAS_API_TOKEN_{letter}` where letter is an uppercase letter `A-Z`. In theory, we can have 26 operators if we wanted, but the tokens have to be created and added to the airflow environment manually. **Note:** "The API is rate limited based on the API token (not the account, instance, or user)" ([API Rate Limiting](https://community.canvaslms.com/t5/Canvas-Developers-Group/API-Rate-Limiting/ba-p/255845))
    - The number of parallel operators is set by the `PARALLEL_SPLITS` varaible in the `dag.py` file. 
