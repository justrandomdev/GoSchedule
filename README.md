# GoSchedule

GoSchedule is a golang job scheduler. It currently support Postgres, Redshift, MySql, MS-SQL Server 


### Example schedule.yaml file:

```yaml
sqljobs:
    - name: job1
      query: usp_update_services
      when:
          frequency:
              minute: 1
    - name: job2
      query: usp_update_organisations
      when:
          time: 15:23
          frequency:
              day: 1
```
