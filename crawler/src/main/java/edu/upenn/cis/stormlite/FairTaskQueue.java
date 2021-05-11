package edu.upenn.cis.stormlite;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.upenn.cis.cis455.crawler.streamprocessors.LinkFilterBolt;
import edu.upenn.cis.stormlite.tasks.ITask;

public class FairTaskQueue {
    static Logger log = LogManager.getLogger(FairTaskQueue.class);

    Queue<ClassQueue<ITask>> fairTaskQueue = new ConcurrentLinkedQueue<ClassQueue<ITask>>();
    Map<String, ClassQueue<ITask>> fairTaskMap = new ConcurrentHashMap<String, ClassQueue<ITask>>();
    int linkFilterRemaining = 10;

    public class ClassQueue<T> extends ConcurrentLinkedQueue<T> {

        String className;

        ClassQueue(String name) {
            super();
            className = name;
        }
    }

    public synchronized Queue<ITask> nextQueue() {

        ClassQueue<ITask> taskQueue = fairTaskQueue.peek();

        if (taskQueue == null) {
            return null;
        }

        // We give priority to the LinkFilterBolt - otherwise it acts as a bottleneck.
        if (taskQueue.className.equals(LinkFilterBolt.class.getName())) {
            linkFilterRemaining--;
            if (linkFilterRemaining == 0) {
                linkFilterRemaining = 15;
                fairTaskQueue.poll();
                fairTaskQueue.add(taskQueue);
            }
        } else {
            // Make sure to always add queue back to the end.
            fairTaskQueue.poll();
            fairTaskQueue.add(taskQueue);
        }

        return taskQueue;
    }

    public synchronized Queue<ITask> getQueue(String className) {
        return fairTaskMap.get(className);
    }

    public synchronized void addClass(String className) {
        // log.info("Adding class: " + className);
        ClassQueue<ITask> taskQueue = new ClassQueue<ITask>(className);
        fairTaskMap.put(className, taskQueue);
        fairTaskQueue.add(taskQueue);
    }

    public synchronized void addTask(String className, ITask task) {
        // log.info("Adding task to " + className);
        fairTaskMap.get(className).add(task);
    }

    public synchronized String toString() {
        String res = "FAIR TASK QUEUE:[";
        int size = 0;
        for (String key : fairTaskMap.keySet()) {
            res += key + "=" + fairTaskMap.get(key).size() + ",";
            size += fairTaskMap.get(key).size();
        }
        res = res.substring(0, res.length() - 1) + "]\t";
        res += "size=" + size;
        return res;
    }
}
