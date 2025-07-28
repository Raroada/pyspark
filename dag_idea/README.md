Idea behind this is to play with the idea of creating a dag using only linked lists based on SQL query inputs. From there one could use the resulting linked list object to process SQL queries in order based on the dependices detected.

TODO: 
Add logic that once item 'x' is done, call the next object while incrementing the counter on the object. Once object counter and dependices counter is the same start 'processing'

Check for race condition with the above case

Check to see is python can handle string splits nativly better