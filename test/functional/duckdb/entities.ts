import { Column, Entity, PrimaryColumn } from "../../../src"

@Entity()
export class Post {
    @PrimaryColumn()
    id: number

    @Column()
    title: string

    @Column()
    text: string

    @Column()
    index: number
}
