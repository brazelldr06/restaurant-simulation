restaurant: restaurant.c restaurant_log.c restaurant.h restaurant_log.h

	gcc -Wall -Wextra -g -o restaurant restaurant.c restaurant_log.c -lpthread

clean:
	rm -f restaurant simulation.log
