import argv
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor

pub type Actor {
  Actor(
    id: Int,
    neighbors: List(Int),
    rumor_count: Int,
    s: Float,
    w: Float,
    stable_rounds: Int,
  )
}

// Find the smallest integer 'size' with size^3 >= n
pub fn cube_size(n: Int, size: Int) -> Int {
  case size * size * size >= n {
    True -> size
    False -> cube_size(n, size + 1)
  }
}

// Build a 3D grid for num_nodes nodes.
// Nodes are enumerated row-major with indices 0..num_nodes-1 inside a cube of side 'size'.
// Each node may have up to 6 neighbors: ±x, ±y, ±z. We drop neighbors with index >= num_nodes
pub fn build_3d(num_nodes: Int) -> List(Actor) {
  let size = cube_size(num_nodes, 1)

  // map across exactly num_nodes ids (0..num_nodes-1)
  let ids = list.range(0, num_nodes - 1)

  list.map(ids, fn(id) {
    // x = id % size
    let assert Ok(x) = int.modulo(id, by: size)

    // y = (id / size) % size
    let y_div = id / size
    let assert Ok(y) = int.modulo(y_div, by: size)

    // z = id / (size * size) using integer division
    let z = id / { size * size }

    // build neighbor indices; shadowing 'neighbors' each time is idiomatic
    let neighbors: List(Int) = []

    let neighbors = case x > 0 {
      True -> {
        let left_neighbor = z * size * size + y * size + x - 1
        [left_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case x < size - 1 {
      True -> {
        let right_neighbor = z * size * size + y * size + x + 1
        [right_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case y > 0 {
      True -> {
        let y_coord = y - 1
        let up_neighbor = z * size * size + y_coord * size + x
        [up_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case y < size - 1 {
      True -> {
        let y_coord = y + 1
        let down_neighbor = z * size * size + y_coord * size + x
        [down_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case z > 0 {
      True -> {
        let z_coord = z - 1
        let back_neighbor = z_coord * size * size + y * size + x
        [back_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case z < size - 1 {
      True -> {
        let z_coord = z + 1
        let front_neighbor = z_coord * size * size + y * size + x
        [front_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    // Remove any neighbors that exceed the requested node count (happens because cube may be larger)
    let neighbors = list.filter(neighbors, fn(n) { n < num_nodes })

    // initialize Actor: rumor_count=0, s = id as float, w = 1.0, stable_rounds = 0
    Actor(id, neighbors, 0, int.to_float(id), 1.0, 0)
  })
}

// Deterministic LCG-based choice for an "extra" neighbor so we don't need an external RNG.
// This returns a value in [0, num_nodes-1] and ensures it's not equal to 'id'.
fn pick_extra_neighbor(id: Int, num_nodes: Int) -> Int {
  // small LCG constants (deterministic, cheap)
  let seed = id * 1_664_525 + 1_013_904_223
  let assert Ok(r0) = int.modulo(seed, by: num_nodes)

  case r0 == id {
    True -> {
      // fallback: (r0 + 1) % num_nodes
      let assert Ok(r1) = int.modulo(r0 + 1, by: num_nodes)
      r1
    }
    False -> r0
  }
}

// Build imperfect 3D: same neighbors as build_3d plus one extra (pseudo-random) neighbor
pub fn build_imperfect_3d(num_nodes: Int) -> List(Actor) {
  let base = build_3d(num_nodes)

  list.map(base, fn(actor) {
    case actor {
      Actor(id, neighbors, rumor_count, s, w, stable_rounds) -> {
        let extra = pick_extra_neighbor(id, num_nodes)
        // prepend extra neighbor
        let new_neighbors = [extra, ..neighbors]
        Actor(id, new_neighbors, rumor_count, s, w, stable_rounds)
      }
    }
  })
}

// Build full network: every node is connected to every other node
pub fn build_full_network(num_nodes: Int) -> List(Actor) {
  let ids = list.range(0, num_nodes - 1)

  list.map(ids, fn(id) {
    // All nodes except self are neighbors
    let neighbors = list.filter(ids, fn(n) { n != id })
    Actor(id, neighbors, 0, int.to_float(id), 1.0, 0)
  })
}

// Build line network: nodes connected in a line (0-1-2-3-...)
pub fn build_line(num_nodes: Int) -> List(Actor) {
  let ids = list.range(0, num_nodes - 1)

  list.map(ids, fn(id) {
    let neighbors: List(Int) = []

    // Add left neighbor if exists
    let neighbors = case id > 0 {
      True -> [id - 1, ..neighbors]
      False -> neighbors
    }

    // Add right neighbor if exists  
    let neighbors = case id < num_nodes - 1 {
      True -> [id + 1, ..neighbors]
      False -> neighbors
    }

    Actor(id, neighbors, 0, int.to_float(id), 1.0, 0)
  })
}

// Message types for the gossip actor
pub type GossipMessage {
  Rumor(rumor_value: Float)
  RequestSum
  SumResponse(sum: Float, count: Int)
  StartGossip
  StopGossip
}

// Actor state for gossip process
pub type GossipState {
  GossipState(
    actor: Actor,
    all_actors: List(Actor),
    actor_subjects: List(process.Subject(GossipMessage)),
    convergence_threshold: Int,
    is_converged: Bool,
  )
}

// Main gossip actor behavior
pub fn gossip_actor_handle(
  state: GossipState,
  message: GossipMessage,
) -> actor.Next(GossipState, GossipMessage) {
  case message {
    Rumor(rumor_value) -> {
      case state {
        GossipState(actor, all_actors, actor_subjects, threshold, converged) -> {
          // Update actor with new rumor
          let sum = actor.s +. rumor_value
          let new_s = sum /. 2.0
          let new_stable_rounds = case actor.s == rumor_value {
            True -> actor.stable_rounds + 1
            False -> 0
          }

          let updated_actor =
            Actor(
              actor.id,
              actor.neighbors,
              actor.rumor_count + 1,
              new_s,
              actor.w +. 1.0,
              new_stable_rounds,
            )

          let new_state =
            GossipState(
              updated_actor,
              all_actors,
              actor_subjects,
              threshold,
              converged,
            )

          // Check for convergence
          let is_converged = updated_actor.stable_rounds >= threshold

          case is_converged && !converged {
            True -> {
              // Send stop message to all actors
              list.each(actor_subjects, fn(subject) {
                process.send(subject, StopGossip)
              })

              actor.continue(GossipState(
                updated_actor,
                all_actors,
                actor_subjects,
                threshold,
                True,
              ))
            }
            False -> actor.continue(new_state)
          }
        }
      }
    }

    StartGossip -> {
      case state {
        GossipState(actor, all_actors, actor_subjects, threshold, converged) -> {
          case !converged {
            True -> {
              // Pick a random neighbor and send rumor
              case list.length(actor.neighbors) {
                0 -> actor.continue(state)
                _ -> {
                  let neighbor_index = actor.id % list.length(actor.neighbors)
                  // Use pattern matching to get the neighbor
                  case list.drop(actor.neighbors, neighbor_index) {
                    [neighbor_id, ..] -> {
                      case list.drop(actor_subjects, neighbor_id) {
                        [neighbor_subject, ..] -> {
                          process.send(neighbor_subject, Rumor(actor.s))
                          actor.continue(state)
                        }
                        [] -> actor.continue(state)
                      }
                    }
                    [] -> actor.continue(state)
                  }
                }
              }
            }
            False -> actor.continue(state)
          }
        }
      }
    }

    StopGossip -> {
      case state {
        GossipState(actor, all_actors, actor_subjects, threshold, converged) -> {
          actor.continue(GossipState(
            actor,
            all_actors,
            actor_subjects,
            threshold,
            True,
          ))
        }
      }
    }

    RequestSum -> {
      case state {
        GossipState(actor, all_actors, actor_subjects, threshold, converged) -> {
          // This would be used for collecting final statistics
          actor.continue(state)
        }
      }
    }

    SumResponse(sum, count) -> {
      // This would be used for collecting final statistics
      actor.continue(state)
    }
  }
}

// Parse command line arguments
fn parse_args(args: List(String)) -> Result(#(Int, String), String) {
  case args {
    [num_nodes_str, topology] -> {
      case int.parse(num_nodes_str) {
        Ok(num_nodes) -> {
          case num_nodes <= 0 {
            True -> Error("Number of nodes must be positive")
            False -> Ok(#(num_nodes, topology))
          }
        }
        Error(_) -> Error("Invalid number of nodes")
      }
    }
    _ ->
      Error(
        "Usage: gleam run <num_nodes> <topology>\nTopologies: full, line, 3d, imperfect3d",
      )
  }
}

// Create actors based on topology
fn create_actors(
  num_nodes: Int,
  topology: String,
) -> Result(List(Actor), String) {
  case topology {
    "full" -> Ok(build_full_network(num_nodes))
    "line" -> Ok(build_line(num_nodes))
    "3d" -> Ok(build_3d(num_nodes))
    "imperfect3d" -> Ok(build_imperfect_3d(num_nodes))
    _ -> Error("Unknown topology. Use: full, line, 3d, or imperfect3d")
  }
}

// Start all gossip actors
fn start_gossip_actors(
  actors: List(Actor),
  convergence_threshold: Int,
) -> Result(List(process.Subject(GossipMessage)), String) {
  // First, create all actors to get their subjects
  let actor_results =
    list.map(actors, fn(actor) {
      let initial_state =
        GossipState(
          actor,
          actors,
          [],
          // Will be filled after all actors are created
          convergence_threshold,
          False,
        )

      actor.new(initial_state)
      |> actor.on_message(gossip_actor_handle)
      |> actor.start
    })

  // Extract subjects from successful starts
  let actor_subjects =
    list.filter_map(actor_results, fn(result) {
      case result {
        Ok(actor.Started(pid: _, data: subject)) -> Ok(subject)
        Error(_) -> Error(Nil)
      }
    })

  Ok(actor_subjects)
}

// Run the gossip simulation
fn run_simulation(
  actors: List(Actor),
  actor_subjects: List(process.Subject(GossipMessage)),
  convergence_threshold: Int,
) -> Nil {
  // Start the gossip by sending StartGossip to all actors
  list.each(actor_subjects, fn(subject) { process.send(subject, StartGossip) })

  // Wait for convergence (simplified - in practice you'd use proper synchronization)
  // For now, we'll just wait a bit and then stop
  process.sleep(1000)
  // 1 second

  // Send stop to all actors
  list.each(actor_subjects, fn(subject) { process.send(subject, StopGossip) })

  // Print results
  io.println("Gossip simulation completed")
}

// Main function
pub fn main() -> Nil {
  let args = argv.load().arguments

  case parse_args(args) {
    Ok(#(num_nodes, topology)) -> {
      case create_actors(num_nodes, topology) {
        Ok(actors) -> {
          let convergence_threshold = 3
          // Default convergence threshold

          case start_gossip_actors(actors, convergence_threshold) {
            Ok(actor_subjects) -> {
              io.println("Starting gossip simulation...")
              io.println("Number of nodes: " <> int.to_string(num_nodes))
              io.println("Topology: " <> topology)
              io.println(
                "Convergence threshold: "
                <> int.to_string(convergence_threshold),
              )

              run_simulation(actors, actor_subjects, convergence_threshold)
            }
            Error(msg) -> io.println("Error starting actors: " <> msg)
          }
        }
        Error(msg) -> io.println("Error creating actors: " <> msg)
      }
    }
    Error(msg) -> io.println("Error: " <> msg)
  }
}
