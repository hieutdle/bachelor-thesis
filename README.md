Project structure


```
bachelor-thesis
└───model                                # dbt model build
|   | core                               # Transform Code for core table
|   | staging                            # Transform Code for View 
|
└───airflow                              # Airflow code
|   | dags                               # Python Code for dags
|   | scripts                            # Support Code for Airflow
|   
└───ce_setup                             # Shell Script for Compute Engine Application installation
|                           
| 
└───superset                             # SQL Scripts for Visualization
```
