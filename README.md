# ucsdlocs
A summary dashboard of busyness of locations on the UCSD campus.

### What is this project?
This project was something I wanted to work on to get familiar with cloud computing (AWS) and the tools available to me on AWS/Tableau. It was also a way for me to learn more about how busy places on campus are during times of the day.

### What tools did I use?

Python, requests, pandas, AWS Lambda, AWS S3 (for storage), AWS CloudBridge Events (for scheduler), AWS Athena, AWS Glue, Tableau

### How did the project work?

I first created the local foundations of the project on my local machine (e.g. scraping locally and running on a schedule via cron job) to create the data files. I then utilized the same code in an AWS Lambda function that scraped data from waitz. This data was then ingested into S3, where I then used AWS Glue to create tables for each respective location on campus into a database that was to be used with AWS Athena. Finally, I connected Athena to Tableau and created a visualization which you can check out here: https://public.tableau.com/app/profile/kelvin.nguyen7871/viz/geiseldash/Dashboard1.

### Challenges

There were a few challenges I faced whilst working on this project, primarily just understanding how AWS worked and how the tools connected with one another. I actually completely messed up one of my sb functions in AWS Lambda, causing the main location files to not have any proper data until I fixed it.

## Future Considerations

- I ran out of requests for the free tier of AWS for S3, so in the future I would likely increase this (e.g. by just paying). Because of this limitation, the data is only limited to a day (for most locations).
- Continuously run the function for a quarter completely on the cloud to analyze quarterly trends for locations on campus
- Create additional dashboards for other locations on campus
