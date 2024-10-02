package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch.
// Параметры
// ctx - контекст
// ch - канал, куда будут отправлены числа
// fn - функция, которая будет вызываться для каждого сгенерированного числа
// после записи в канал. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	defer close(ch) // перед выходом из функции закрываем канал ch

	var current int64 = 1 // текущее число, которое будет отправлено в канал (будет изменяться в течение рантайма)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ch <- current
			fn(current)
			current++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
// Параметры
// in - канал, откуда будут прочитаны числа
// out - канал, куда будут числа записаны
func Worker(in <-chan int64, out chan<- int64) {
	defer close(out) // перед выходом из функции закрываем канал out

	for {
		v, ok := <-in
		if !ok {
			return
		}
		// отправляем полученное число в канал out
		out <- v
		// делаем паузу в 1 мс
		time.Sleep(time.Millisecond)
	}
}

func main() {
	chIn := make(chan int64)

	// создаем контекст типа WithTimeout, который отменится через 1 с
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму
	go Generator(ctx, chIn, func(i int64) {
		atomic.AddInt64(&inputSum, i)   // прибавляем i к inputSum
		atomic.AddInt64(&inputCount, 1) // прибавляем i к inputCount
	})

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup
	// увеличиваем счетчик wg на количество обрабатывающих горутин
	wg.Add(NumOut)

	// 4. Собираем числа из каналов outs
	for i, c := range outs {

		go func(in <-chan int64, i int64) {
			// по завершении работы горутины уменьшаем счетчик wg на 1
			defer wg.Done()

			for {
				v, ok := <-c
				if !ok {
					return
				}
				chOut <- v
				amounts[i]++
			}

		}(c, int64(i))
	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count int64 // количество чисел результирующего канала
	var sum int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала
	for v := range chOut {
		count++
		sum += v
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
