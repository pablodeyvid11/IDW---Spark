package br.dev.ppaiva.idw4spark.util;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TimeTracker implements Serializable {
	private static final long serialVersionUID = 1L;

	private Optional<Long> start;
	private Optional<Long> end;

	public TimeTracker() {
		this.start = Optional.empty();
		this.end = Optional.empty();
	}

	public void start() {
		this.start = Optional.of(System.currentTimeMillis());
		this.end = Optional.empty();
	}

	public void end() {
		if (!start.isPresent()) {
			throw new IllegalStateException("Start time is not set. Please call start() before calling end().");
		}
		this.end = Optional.of(System.currentTimeMillis());
	}

	public long getElapsedTime() {
		if (!start.isPresent() || !end.isPresent()) {
			throw new IllegalStateException("Both start and end times must be set to calculate elapsed time.");
		}
		return end.get() - start.get();
	}

	public void reset() {
		this.start = Optional.empty();
		this.end = Optional.empty();
	}

	public Optional<Long> getStartTime() {
		return start;
	}

	public Optional<Long> getEndTime() {
		return end;
	}

	public String getFormattedElapsedTime() {
		long elapsedTime = getElapsedTime();

		long hours = TimeUnit.MILLISECONDS.toHours(elapsedTime);
		elapsedTime -= TimeUnit.HOURS.toMillis(hours);

		long minutes = TimeUnit.MILLISECONDS.toMinutes(elapsedTime);
		elapsedTime -= TimeUnit.MINUTES.toMillis(minutes);

		long seconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
		elapsedTime -= TimeUnit.SECONDS.toMillis(seconds);

		long milliseconds = elapsedTime;

		return String.format("%02d hours, %02d minutes, %02d seconds and %03d milliseconds", hours, minutes, seconds,
				milliseconds);
	}
}
